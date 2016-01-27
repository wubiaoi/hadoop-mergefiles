package com.jd.bdp.hdfs.mergefiles;

import com.jd.bdp.hdfs.mergefiles.exception.FileTypeNotUniqueException;
import com.jd.bdp.hdfs.mergefiles.mr.MergePath;
import com.jd.bdp.hdfs.mergefiles.mr.lib.Filter;
import com.jd.bdp.utils.LogHelper;
import com.jd.bdp.utils.Utils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.*;

/**
 * 合并HDFS上的小文件
 * Created by wubiao on 1/18/16.
 */
public class MergeTask implements Task {

  private final static Log log = LogFactory.getLog(MergeTask.class);
  private final LogHelper console;

  private FileSystem fs;
  private Configuration conf;
  private Path tmpDir;
  private Path logDir;

  public MergeTask() {
    console = new LogHelper(log, true);
  }

  @Override
  public void init(String[] args) throws IOException {
    //初始化参数
    Config.init(args);
    conf = new Configuration();
    this.fs = FileSystem.get(conf);
    //创建临时目录
    if (Config.getTmpDir() == null) {
      String user = System.getProperty("user.name");
      String tmp = "/user/" + user + "/warehouse/tmp/sqltmp";
      Config.setTmpDir(new Path(tmp));
      fs.mkdirs(Config.getTmpDir());
    }
    console.printInfo(Config.list());
    //输出合并路径
    StringBuilder res = new StringBuilder();
    res.append("待合并路径" + Config.FIELD_SEPARATOR);
    res.append("类型" + Config.FIELD_SEPARATOR);
    res.append("大小" + Config.FIELD_SEPARATOR);
    res.append("总文件数");
    res.append("\n");
    console.printInfo(res.toString());
  }

  @Override
  public int run() throws IOException, InterruptedException {
    Path[] path = Config.getPath();
    int errorNumber = 0;
    //打开流将待合并的目录保存到文件
    MergeContext context = new MergeContext();
    //发现需要合并的路径
    if (path == null) {
      File file = new File(Config.getSourefile());
      InputStreamReader in = new InputStreamReader(new FileInputStream(file));
      BufferedReader br = new BufferedReader(in);
      String line;
      while ((line = br.readLine()) != null) {
        Path p = new Path(line);
        if (fs.isDirectory(p)) {
          conf.set(Config.INPUT_DIR, p.toString());
          recurseDir(p, context, fs);
        } else {
          console.printError("请输入一个合法的路径");
          System.exit(1);
        }
      }
    } else {
      for (Path p : path) {
        if (fs.isDirectory(p)) {
          conf.set(Config.INPUT_DIR, p.toString());
          recurseDir(p, context, fs);
        } else {
          console.printError("不是一个合法的路径 " + p);
          System.exit(1);
        }
      }
    }
    //合并小文件
    MergePath task;
    StringBuilder report = new StringBuilder();
    console.printInfo("总计合并路径个数为: " + context.getTotal());
    while (context.isRunning()) {
      while ((task = context.getRunnable(Config.getMaxJob())) != null) {
        TaskRunner taskRunner = null;
        try {
          taskRunner = new TaskRunner(task, context);
          context.launching(taskRunner);
        } catch (IOException e) {
          console.printError(task.getPath() + "==>init TaskRunner failed." + ExceptionUtils.getFullStackTrace(e));
          continue;
        }
        taskRunner.start();
      }
      TaskRunner mergeRunner = context.pollFinished();
      if (mergeRunner == null) {
        continue;
      }
      if (mergeRunner.getException() != null) {
        console.printError(mergeRunner.getPREFIX() + " ERROR! " + ExceptionUtils.getFullStackTrace(mergeRunner.getException()));
        errorNumber++;
        mergeRunner.errorRecorder(mergeRunner.getInput() + "\t"
                + mergeRunner.getInputType() + "\t" + mergeRunner.getOutput() + "\t" + mergeRunner.getJobstatus() + "\t" + Config.getMergeTargePath() + "\n");
      } else {
        ContentSummary mergeAfter = fs.getContentSummary(mergeRunner.getTargetPath());
        report.append("\n=========[" + mergeRunner.getInput() + "]=============");
        report.append("\n\t合并信息:\n");
        report.append(String.format("\t\t合并前:Size=%s,FileCount=%s",
                mergeRunner.getMergePath().getSize(), mergeRunner.getMergePath().getNumFiles()) + "\n");
        report.append(String.format("\t\t合并后:Size=%s,FileCount=%s",
                mergeAfter.getLength(), mergeAfter.getFileCount()) + "\n");
        report.append("\t\tInput Records:" + mergeRunner.getMergePath().getMapInput());
        report.append("\t\tOutput Records:" + mergeRunner.getMergePath().getMapOutput());
      }
      mergeRunner.shutdown();
    }
    //输出合并信息
    console.printInfo(report.toString());
    if (errorNumber != 0) {
      console.printError("发现有" + errorNumber + "路径合并失败");
    }
    return 0;
  }

  /**
   * 递归获取所有需要合并的目录
   *
   * @param path
   * @param context
   * @param fs
   * @throws InterruptedException
   */
  public void recurseDir(Path path, MergeContext context, FileSystem fs)
          throws InterruptedException, IOException {
    FileStatus[] dirs = fs.listStatus(path, new Filter.MergeDirFilter());
    if (dirs.length > 0) { //当目录下存在子目录
      if (Config.isRecursive()) {
        for (FileStatus dir : dirs) {
          recurseDir(dir.getPath(), context, fs);
        }
      }
    } else { //存在需要合并的文件
      FileStatus[] files = fs.listStatus(path, new Filter.MergeFileFilter());
      ContentSummary contentSummary = fs.getContentSummary(path);
      long size = contentSummary.getLength();
      long totalCount = contentSummary.getFileCount();
      long fileCount = files.length;
      if (fileCount > 1 && size / fileCount < Config.getMergeLessThanSize()) {
        //创建临时目录
        String tmpDirName = Utils.cutPrefix(
                Path.getPathWithoutSchemeAndAuthority(path).toString()
                        .replaceAll("/", "_"), "_");
        tmpDirName = tmpDirName + "_" + Utils.ts();
        Path curDir = new Path(Utils.dt(), tmpDirName);
        tmpDir = new Path(Config.getTmpDir(), curDir);
        logDir = new Path(Config.getTmpDir(),
                new Path(new Path("logs", Utils.dt()), tmpDirName));
        fs.mkdirs(logDir);

        FileType type;
        try {
          type = Filter.checkTypeUnique(files, fs);
        } catch (FileTypeNotUniqueException e) {
          FSDataOutputStream errout = fs.create(new Path(logDir, "error.log"));
          errout.writeBytes("TypeNotUnique\t" + path);
          errout.close();
          return;
        }
        MergePath mergePath = new MergePath(path, type, size, totalCount);
        mergePath.setTmpDir(tmpDir);
        mergePath.setLogDir(logDir);
        context.addToRunnable(mergePath);
        StringBuilder res = new StringBuilder();
        res.append(path.toString() + Config.FIELD_SEPARATOR);
        res.append(type + Config.FIELD_SEPARATOR);
        res.append(size + Config.FIELD_SEPARATOR);
        res.append(totalCount);
        res.append("\n");
        console.printInfo(res.toString());
      }

    }
  }

  @Override
  public void close() throws IOException {
  }
}
