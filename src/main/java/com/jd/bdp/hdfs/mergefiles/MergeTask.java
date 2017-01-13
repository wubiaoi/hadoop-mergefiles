package com.jd.bdp.hdfs.mergefiles;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jd.bdp.hdfs.mergefiles.exception.FileTypeNotUniqueException;
import com.jd.bdp.hdfs.mergefiles.mapreduce.MergePath;
import com.jd.bdp.hdfs.mergefiles.mapreduce.lib.Filter;
import com.jd.bdp.utils.LogHelper;
import com.jd.bdp.utils.Utils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;

import java.io.*;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;

/**
 * 合并HDFS上的小文件
 * Created by wubiao on 1/18/16.
 */
public class MergeTask implements Task {

  private final static Log log = LogFactory.getLog(MergeTask.class);
  private final LogHelper console;

  private FileSystem fs;
  private Configuration conf;
  private ExecutorService threadPool;
  private Set<String> excludePaths;
  private ConcurrentLinkedQueue<Future> futures = new ConcurrentLinkedQueue<Future>();

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
      String user = Utils.USER;
      String tmp = "/user/" + user + "/warehouse/tmp/sqltmp";
      Config.setTmpDir(new Path(tmp));
    }
    if (!fs.exists(Config.getTmpDir())) {
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
    threadPool = Executors.newFixedThreadPool(Config.getSplitNumThreads(), new ThreadFactoryBuilder().setDaemon(true).build());
    excludePaths = new HashSet<String>();
    console.printInfo(res.toString());
  }

  @Override
  public int run() throws IOException, InterruptedException, ExecutionException {
    Path[] path = Config.getPath();
    int errorNumber = 0;
    //打开流将待合并的目录保存到文件
    MergeContext context = new MergeContext();
    //读取不需要合并的路径
    if (Config.getExcludePath() != null) {
      File file = new File(Config.getExcludePath());
      InputStreamReader in = new InputStreamReader(new FileInputStream(file));
      BufferedReader br = new BufferedReader(in);
      String line;
      while ((line = br.readLine()) != null) {
        excludePaths.add(line);
      }
    }
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
          console.printError(line + " 不合法!请输入一个合法的路径");
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

    // wait all threads finish
    for (Future future : futures) {
      future.get();
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
      if (mergeRunner.getException() != null || mergeRunner.getResult() != 0) {
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
  public void recurseDir(Path path, final MergeContext context, final FileSystem fs) {
    if (isExcludePath(path, excludePaths)) return;
    FileStatus[] dirs = null;
    try {
      dirs = fs.listStatus(path, new Filter.MergeDirFilter());
    } catch (IOException e) {
      console.printError(ExceptionUtils.getFullStackTrace(e));
      System.exit(-1);
    }
    if (dirs.length > 0) { //当目录下存在子目录
      if (Config.isRecursive()) {
        for (final FileStatus dir : dirs) {
          if (isExcludePath(dir.getPath(), excludePaths)) continue;
          futures.add(threadPool.submit(new Runnable() {
            @Override
            public void run() {
              recurseDir(dir.getPath(), context, fs);
            }
          }));
        }
      }
    } else { //存在需要合并的文件
      Path stage = new Path(path, ".stage");
      try {
        if (fs.exists(stage)) {
          fs.delete(stage);
        }
      } catch (IOException e) {
        log.warn("clear tmp .stage failed." + stage.toString());
      }
      FileStatus[] files = null;
      ContentSummary contentSummary = null;
      try {
        files = fs.listStatus(path, new Filter.MergeFileFilter());
        contentSummary = fs.getContentSummary(path);
      } catch (IOException e) {
        console.printError(ExceptionUtils.getFullStackTrace(e));
        System.exit(-1);
      }
      long size = contentSummary.getLength();
      long totalCount = contentSummary.getFileCount();
      long fileCount = files.length;
      if (fileCount > 1 && size / fileCount < Config.getMergeLessThanSize()) {
        //创建临时目录
        Path tmpDir;
        Path logDir;
        String tmpDirName = Utils.cutPrefix(
                Path.getPathWithoutSchemeAndAuthority(path).toString()
                        .replaceAll("/", "_"), "_");
        tmpDirName = tmpDirName + "_" + Utils.ts();
        Path curDir = new Path(Utils.dt(), tmpDirName);
        tmpDir = new Path(Config.getTmpDir(), curDir);
        logDir = new Path(Config.getTmpDir(),
                new Path(new Path("logs", Utils.dt()), tmpDirName));

        FileType type = null;
        try {
          if (!fs.exists(logDir)) {
            fs.mkdirs(logDir);
          }
          type = Filter.checkTypeUnique(files, fs);
        } catch (FileTypeNotUniqueException e) {
          try {
            FSDataOutputStream errout = fs.create(new Path(logDir, "error.log"));
            errout.writeBytes("TypeNotUnique\t" + path);
            errout.close();
          } catch (IOException e1) {
            log.warn(ExceptionUtils.getFullStackTrace(e1));
          }
          return;
        } catch (IOException e) {
          console.printError(ExceptionUtils.getFullStackTrace(e));
          System.exit(-2);
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

  /**
   * 检查路径是否在exclude path中
   *
   * @param path
   * @param excludePaths
   * @return
   */
  private static boolean isExcludePath(Path path, Set<String> excludePaths) {
    if (excludePaths == null || excludePaths.isEmpty()) return false;
    for (String excludePath : excludePaths) {
      if (path.toString().startsWith(excludePath)) {
        return true;
      }
    }
    return false;
  }
}
