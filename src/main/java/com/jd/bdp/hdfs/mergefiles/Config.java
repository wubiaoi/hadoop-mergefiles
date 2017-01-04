package com.jd.bdp.hdfs.mergefiles;

import com.jd.bdp.utils.Utils;
import com.jd.bdp.utils.VersionInfo;
import org.apache.commons.cli.*;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
  private static Path[] path = null;
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
   * 读取本地文件获取合并目录
   */
  private static String sourefile;

  /**
   * 每个合并任务的临时目录
   */
  private static Path tmpDir;

  /**
   * 日志文件位置
   */
  private static Path logDir;

  /**
   * 合并后文件的目录
   */
  private static Path mergeTargePath;

  private static boolean wantNoneTypeToText;

  private static FileType fileType;

  private static String excludePath;

  private static int splitNumThreads = 50;

  public static FileType getFileType() {
    return fileType;
  }

  public static void setFileType(FileType fileType) {
    Config.fileType = fileType;
  }

  public static boolean isWantNoneTypeToText() {
    return wantNoneTypeToText;
  }

  public static void setWantNoneTypeToText(boolean wantNoneTypeToText) {
    Config.wantNoneTypeToText = wantNoneTypeToText;
  }

  public static Path getLogDir() {
    return logDir;
  }

  public static void setLogDir(Path logDir) {
    Config.logDir = logDir;
  }

  public static String getSourefile() {
    return sourefile;
  }

  public static void setSourefile(String sourefile) {
    Config.sourefile = sourefile;
  }

  public static int getMaxJob() {
    return maxJob;
  }

  public static void setMaxJob(int maxJob) {
    if (maxJob < 1) {
      System.out.println("job数必须大于１");
      System.exit(1);
    }
    Config.maxJob = maxJob;
  }

  public static Path getMergeTargePath() {
    return mergeTargePath;
  }

  public static void setMergeTargePath(Path mergeTargePath) {
    Config.mergeTargePath = mergeTargePath;
  }

  public static Path[] getPath() {
    return path;
  }

  public static void setPath(Path[] path) {
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

  public static boolean isRecursive() {
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

  public static String getExcludePath() {
    return excludePath;
  }

  public static void setExcludePath(String excludePath) {
    Config.excludePath = excludePath;
  }

  public static int getSplitNumThreads() {
    return splitNumThreads;
  }

  public static void setSplitNumThreads(int splitNumThreads) {
    Config.splitNumThreads = splitNumThreads;
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
    options.addOption("p", "path", true, "hdfs路径,多个路径以逗号分隔");
    options.addOption("s", "mergeLessThanSize", true, "设置小于size大小的文件将被合并，默认:128M");
    options.addOption("m", "reducesize", true, "设置合并每个reducer处理的大小，默认:250M");
    options.addOption("n", "isRecursive", false, "是否递归合并，默认:递归合并");
    options.addOption("t", "wantNoneTypeToText", false, "确定文件为text,默认:No");
    options.addOption("o", "output", true, "合并结果目录，默认:输入目录");
    options.addOption("f", "file", true, "以文件形式传入路径");
    options.addOption("j", "maxJobNum", true, "最大并发的job数");
    options.addOption("d", "tempDir", true, "临时目录");
    options.addOption("u", "fileType", true, "指定文件格式(orc,lzo,text,avro等)");
    options.addOption("x", "excludePath", true, "指定一个文件,内容为不需要合并的目录");
    options.addOption("l", "splitNumThreads", true, "计算目录起用的最大线程数.");
    CommandLineParser parser = new PosixParser();
    CommandLine cmd = null;

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
      String paths = cmd.getOptionValue('p');
      String[] ps = StringUtils.split(paths, ',');
      if (ps != null) {
        path = new Path[ps.length];
        for (int i = 0; i < path.length; i++) {
          path[i] = new Path(ps[i]);
        }
      }
    } else if (cmd.hasOption('f')) {
      Config.setSourefile(cmd.getOptionValue('f'));
    } else {
      System.out.println("必须输入合并的HDFS全路径或者指定个本地文件");
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
    if (cmd.hasOption('j')) {
      Config.setMaxJob(Integer.parseInt(cmd.getOptionValue('j')));
    }
    if (cmd.hasOption('d')) {
      Config.setTmpDir(new Path(cmd.getOptionValue('d')));
    }
    if (cmd.hasOption('u')) {
      FileType type = null;
      try {
        type = FileType.valueOf(cmd.getOptionValue('u').toUpperCase());
      } catch (IllegalArgumentException e) {
        throw e;
      } catch (NullPointerException ne) {
      }
      Config.setFileType(type);
    }
    if (cmd.hasOption('x')) {
      Config.setExcludePath(cmd.getOptionValue('x')); // 排除一些不需要合并的目录
    }
    if (cmd.hasOption('l')) {
      Config.setSplitNumThreads(Integer.parseInt(cmd.getOptionValue('l')));
    }
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
    conf.setBoolean(LocalConf.MERGE_IS_RECURSIVE.name(), isRecursive());
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
            "版本:" + VersionInfo.version + "\n" +
            "Usage: hadoop jar MergeTask -p 准备合并的HDFS全路径\n" +
            "高级参数说明：\n" +
            "\t-p\t指定准备合并的HDFS全路径,多个以逗号分隔，必选\n" +
            "\t-s\t设置小于size大小的文件将被合并，默认:128M\n" +
            "\t-m\t设置合并每个reducer处理的大小，默认:250M\n" +
            "\t-n\t是否递归合并，默认:递归合并\n" +
            "\t-t\t是否把不识别的文件认为是文本类型，默认:否\n" +
            "\t-o\t设置合并后输出的HDFS路径，默认:输入目录\n" +
            "\t-f\t指定一个本地文件作为输入合并目录\n" +
            "\t-j\t指定最大并行的job数\n" +
            "\t-d\t指定合并临时目录\n" +
            "\t-u\t指定文件格式(orc,lzo,text,avro等)\n" +
            "\t-x\t指定一个文件,内容为不需要合并的目录\n" +
            "\t-l\t计算目录起用的最大线程数.\n" +
            "\t";
  }

  public static String list() {
    StringBuilder msg = new StringBuilder();
    msg.append("合并程序版本: " + VersionInfo.version);
    msg.append("输入参数:\n");
    msg.append("\t合并路径: " + Arrays.deepToString(path) + "\n");
    msg.append("\t小文件合并小于: " + mergeLessThanSize / 1024 / 1024 + " Mb\n");
    msg.append("\t合并后文件的平均大小: " + mergeMaxSize / 1024 / 1024 + "Mb\n");
    msg.append("\t是否递归合并: " + ((isRecursive) ? "是\n" : "否\n"));
    msg.append("\t临时目录: " + tmpDir + "\n");
    msg.append("\t合并后文件的目录: " + (mergeTargePath == null ? Arrays.deepToString(path) : mergeTargePath) + "\n");
    msg.append("\t最大并发JOB数: " + maxJob + "\n");
    msg.append("\t不识别文件类型处理: " + (wantNoneTypeToText ? "认为是Text" : "不处理") + "\n");
    msg.append("\t文件读取输入路径文件路径: " + sourefile + "\n");
    msg.append("\tExclude Path路径: " + excludePath + "\n");
    msg.append("\t计算目录起用的最大线程数: " + splitNumThreads + "\n");
    msg.append("\t当前用户: " + Utils.USER + "\n");
    return msg.toString();
  }
}
