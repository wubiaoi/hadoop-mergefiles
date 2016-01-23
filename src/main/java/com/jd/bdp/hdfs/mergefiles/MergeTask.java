package com.jd.bdp.hdfs.mergefiles;

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

  public MergeTask() {
    console = new LogHelper(log, true);
  }

  @Override
  public void init(String[] args) throws IOException {
    //初始化参数
    Config.init(args);
    conf = new Configuration();
    this.fs = FileSystem.get(conf);
    Path path = Config.getPath();
    //创建临时目录
    tmpDir = new Path(new Path(fs.getHomeDirectory().toString(), "tmp").toString() +
            "/.merge/." + Utils.makeMergeId(fs.resolvePath(path).toString()));
    Config.setTmpDir(tmpDir);
    fs.mkdirs(tmpDir);
    console.printInfo(Config.list());
  }

  @Override
  public int run() throws IOException, InterruptedException {
    Path path = Config.getPath();
    int errorNumber = 0;
    //打开流将待合并的目录保存到文件
    Path mergeListFile = new Path(Config.getTmpDir(), Config.MERGE_PATH_LIST_FILE);
    FSDataOutputStream out = fs.create(mergeListFile);
    console.printInfo("Create Temp dir [" + mergeListFile + "] success");
    MergeContext context = new MergeContext();
    //发现需要合并的路径
    if (path == null) {
      File file = new File(Config.getSourefile());
      InputStreamReader in = new InputStreamReader(new FileInputStream(file));
      BufferedReader br = new BufferedReader(in);
      String line;
      while ((line = br.readLine()) != null) {
        path = new Path(line);
        if (fs.isDirectory(path)) {
          conf.set(Config.INPUT_DIR, path.toString());
          recurseDir(path, context, fs, out);
        } else {
          console.printError("请输入一个合法的路径");
          System.exit(1);
        }
      }
    } else if (fs.isDirectory(path)) {
      conf.set(Config.INPUT_DIR, path.toString());
      recurseDir(path, context, fs, out);
    } else {
      console.printError("请输入一个合法的路径");
      System.exit(1);
    }
    out.close();
    Path errorFile = new Path(tmpDir, "error.log");
    FSDataOutputStream errOut = fs.create(errorFile);
    //合并小文件
    MergePath task;
    StringBuilder report = new StringBuilder();
    console.printInfo("总计合并路径个数为: " + context.getTotal());
    while (context.isRunning()) {
      while ((task = context.getRunnable(Config.getMaxJob())) != null) {
        TaskRunner taskRunner = new TaskRunner(task, context);
        taskRunner.start();
      }
      TaskRunner mergeRunner = context.pollFinished();
      if (mergeRunner == null) {
        continue;
      }
      if (mergeRunner.getException() != null) {
        console.printError(mergeRunner.getPREFIX() + " ERROR! " + ExceptionUtils.getFullStackTrace(mergeRunner.getException()));
        errorNumber++;
        errOut.writeBytes(mergeRunner.getInput() + "\t"
                + mergeRunner.getInputType() + "\t" + mergeRunner.getOutput() + "\t" + mergeRunner.getJobstatus() + "\t" + Config.getMergeTargePath() + "\n");
      } else {
        ContentSummary mergeAfter = fs.getContentSummary(mergeRunner.getTargetPath());
        report.append("\n=========[" + mergeRunner.getInput() + "]=============");
        report.append("\n\t合并信息:\n");
        report.append(String.format("\t\t合并前:Size=%s,FileCount=%s",
                mergeRunner.getMergePath().getSize(), mergeRunner.getMergePath().getNumFiles()) + "\n");
        report.append(String.format("\t\t合并后:Size=%s,FileCount=%s",
                mergeAfter.getLength(), mergeAfter.getFileCount()) + "\n");
      }
    }
    console.printInfo(report.toString());
    if (errorNumber != 0) {
      console.printError("发现有" + errorNumber + "路径合并失败,请查看日志:" + errorFile);
    }
    errOut.close();
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
  public static void recurseDir(Path path, MergeContext context, FileSystem fs, FSDataOutputStream out) throws InterruptedException, IOException {
    FileStatus[] files = fs.listStatus(path, new Filter.MergeFileFilter());
    //当前路径下存在小文件,将路径加入到待合并清单
    if (files.length > 1) {
      FileType type = Utils.getFileType(files[0].getPath(), fs);
      ContentSummary cs = fs.getContentSummary(path);
      MergePath mergePath = new MergePath(path, type, cs.getLength(), cs.getFileCount());
      context.addToRunnable(mergePath);
      StringBuilder res = new StringBuilder();
      res.append(path.toString() + Config.FIELD_SEPARATOR);
      res.append(FileType.TEXT + Config.FIELD_SEPARATOR);
      res.append(cs.getLength() + Config.FIELD_SEPARATOR);
      res.append(cs.getFileCount());
      res.append("\n");
      out.writeBytes(res.toString());
    }
    //如果递归合并,则递归子目录,将存在小文件的目录加入到待合并清单
    if (Config.isIsRecursive()) {
      FileStatus[] dirs = fs.listStatus(path, new Filter.MergeDirFilter());
      //如果存在符合条件的子目录
      for (FileStatus dir : dirs) {
        recurseDir(dir.getPath(), context, fs, out);
      }
    }
  }

}
