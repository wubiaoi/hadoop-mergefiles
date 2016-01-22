package com.jd.bdp.hdfs.mergefiles;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 * Created by wubiao on 1/18/16.
 */
public class Config {

  /**
   * 保存合并目录清单的HDFS文件名
   */
  public final static String MERGE_PATH_LIST_FILE = "merge_path_list.log";

  public static final String INPUT_DIR =
          "mapreduce.input.fileinputformat.inputdir";

  public final static String FIELD_SEPARATOR = "\t";

  private static int maxJob = 100;

  /**
   * 指定准备合并的HDFS全路径，必选
   */
  private static Path path = null;
  /**
   * 设置小于size大小的文件将被合并，默认:128M
   */
  private static long mergeLessThanSize = 128 * 1024 * 1024;
  /**
   * 设置合并每个reducer处理的大小，默认:250M
   */
  private static long mergeMaxSize = 250 * 1024 * 1024;
  /**
   * 是否递归合并，默认:递归合并
   */
  private static boolean isRecursive = true;

  /**
   * 每个合并任务的临时目录
   */
  private static Path tmpDir;

  /**
   * 合并后文件的目录
   */
  private static Path mergeTargePath;

  private static boolean wantNoneTypeToText;

  public static boolean isWantNoneTypeToText() {
    return wantNoneTypeToText;
  }

  public static void setWantNoneTypeToText(boolean wantNoneTypeToText) {
    Config.wantNoneTypeToText = wantNoneTypeToText;
  }

  public static int getMaxJob() {
    return maxJob;
  }

  public static void setMaxJob(int maxJob) {
    Config.maxJob = maxJob;
  }

  public static Path getMergeTargePath() {
    return mergeTargePath;
  }

  public static void setMergeTargePath(Path mergeTargePath) {
    Config.mergeTargePath = mergeTargePath;
  }

  public static Path getPath() {
    return path;
  }

  public static void setPath(Path path) {
    Config.path = path;
  }

  public static long getMergeLessThanSize() {
    return mergeLessThanSize;
  }

  public static void setMergeLessThanSize(long mergeLessThanSize) {
    Config.mergeLessThanSize = mergeLessThanSize;
  }

  public static long getMergeMaxSize() {
    return mergeMaxSize;
  }

  public static void setMergeMaxSize(long mergeMaxSize) {
    Config.mergeMaxSize = mergeMaxSize;
  }

  public static boolean isIsRecursive() {
    return isRecursive;
  }

  public static void setIsRecursive(boolean isRecursive) {
    Config.isRecursive = isRecursive;
  }

  public static Path getTmpDir() {
    return tmpDir;
  }

  public static void setTmpDir(Path tmpDir) {
    Config.tmpDir = tmpDir;
  }

  /**
   * 处理命令行输入的参数
   *
   * @param args
   */
  public static void init(String[] args) {
    //参数处理
    Options options = new Options();
    options.addOption("h", "help", false, "查看使用说明");
    options.addOption("p", "path", true, "hdfs路径");
    options.addOption("s", "mergeLessThanSize", true, "设置小于size大小的文件将被合并，默认:128M");
    options.addOption("m", "reducesize", true, "设置合并每个reducer处理的大小，默认:250M");
    options.addOption("n", "isRecursive", false, "是否递归合并，默认:递归合并");
    options.addOption("t", "wantNoneTypeToText", false, "确定文件为text,默认:No");
    options.addOption("o", "isRecursive", true, "合并结果目录，默认:输入目录");
    CommandLineParser parser = new PosixParser();
    CommandLine cmd = null;

    Path path = null; //准备合并的路径

    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      System.out.println("参数有误!" + help());
      System.out.println(e.getMessage());
    }
    if (cmd.hasOption('h')) {
      System.out.println(help());
      System.exit(0);
    }
    if (cmd.hasOption('p')) {
      path = new Path(cmd.getOptionValue('p'));
      Config.setPath(path);
    } else {
      System.out.println("必须输入合并的HDFS全路径!");
      System.out.println(help());
      System.exit(1);
    }
    if (cmd.hasOption('s')) {
      Config.setMergeLessThanSize(Long.parseLong(cmd.getOptionValue('s')) * 1024 * 1024);
    }
    if (cmd.hasOption('m')) {
      Config.setMergeMaxSize(Integer.parseInt(cmd.getOptionValue('m')) * 1024 * 1024);
    }
    if (cmd.hasOption('n')) {
      Config.setIsRecursive(false);
    }
    if (cmd.hasOption('t')) {
      Config.setWantNoneTypeToText(true);
    }
    if (cmd.hasOption('o')) {
      Config.setMergeTargePath(new Path(cmd.getOptionValue('o')));
    }
  }

  /**
   * 保存本地配置信息到 HadoopConf
   *
   * @param conf
   */
  public static void init(Configuration conf) {
    setPath(new Path(conf.get(LocalConf.MERGE_PATH.name())));
    setMergeLessThanSize(conf.getLong(LocalConf.MERGE_FILE_LESS_THAN_SIZE.name(), getMergeLessThanSize()));
    setIsRecursive(conf.getBoolean(LocalConf.MERGE_IS_RECURSIVE.name(), isIsRecursive()));
    setMergeMaxSize(conf.getLong(LocalConf.MERGE_FILE_MAX_SIZE.name(), getMergeMaxSize()));
    setTmpDir(new Path(conf.get(LocalConf.MERGE_TMP_DIR.name())));
    setMergeTargePath(new Path(conf.get(LocalConf.MERGE_TARGET_PATH.name())));
  }

  /**
   * Hadoopconf对应的配置名称
   */
  public enum LocalConf {
    MERGE_PATH, //准备需要合并的路径
    MERGE_FILE_LESS_THAN_SIZE, //小于size的文件将被合并
    MERGE_FILE_MAX_SIZE, //合并后每个文件maxsize
    MERGE_IS_RECURSIVE, //是否递归合并
    MERGE_TMP_DIR, // 任务的临时目录
    MERGE_TARGET_PATH, //　合并后文件的目录
  }


  public static void copyConf(Configuration conf) {
    conf.set(LocalConf.MERGE_PATH.name(), getPath().toString());
    conf.setLong(LocalConf.MERGE_FILE_LESS_THAN_SIZE.name(), getMergeLessThanSize());
    conf.setBoolean(LocalConf.MERGE_IS_RECURSIVE.name(), isIsRecursive());
    conf.setLong(LocalConf.MERGE_FILE_MAX_SIZE.name(), getMergeMaxSize());
    conf.set(LocalConf.MERGE_TMP_DIR.name(), getTmpDir().toString());
    conf.set(LocalConf.MERGE_TARGET_PATH.name(), getMergeTargePath().toString());
  }

  /**
   * 打印Usage
   *
   * @return
   */
  public static String help() {
    return "功能: 合并HDFS一个目录下的小文件，默认小于128M将被合并\n" +
            "Usage: hadoop jar MergeTask -p 准备合并的HDFS全路径\n" +
            "高级参数说明：\n" +
            "\t-p\t指定准备合并的HDFS全路径，必选\n" +
            "\t-s\t设置小于size大小的文件将被合并，默认:128M\n" +
            "\t-m\t设置合并每个reducer处理的大小，默认:250M\n" +
            "\t-n\t是否递归合并，默认:递归合并\n" +
            "\t-t\t是否把不识别的文件认为是文本类型，默认:否\n" +
            "\t-o\t设置合并后输入的路径，默认:输入目录\n" +
            "\t";
  }

  public static String list() {
    StringBuilder msg = new StringBuilder();
    msg.append("输入参数:\n");
    msg.append("\t合并路径: " + path + "\n");
    msg.append("\t小文件合并小于: " + mergeLessThanSize / 1024 / 1024 + " Mb\n");
    msg.append("\t合并后文件的平均大小: " + mergeMaxSize / 1024 / 1024 + "Mb\n");
    msg.append("\t是否递归合并: " + ((isRecursive) ? "是\n" : "否\n"));
    msg.append("\t临时目录: " + tmpDir + "\n");
    msg.append("\t合并后文件的目录: " + (mergeTargePath == null ? path : mergeTargePath) + "\n");
    msg.append("\t最大并发JOB数: " + maxJob + "\n");
    return msg.toString();
  }
}
