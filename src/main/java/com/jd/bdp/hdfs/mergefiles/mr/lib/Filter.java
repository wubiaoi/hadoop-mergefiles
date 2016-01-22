package com.jd.bdp.hdfs.mergefiles.mr.lib;

import com.jd.bdp.hdfs.mergefiles.Config;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import java.io.IOException;

/**
 * Created by wubiao on 1/20/16.
 */
public class Filter {
  /**
   * 小文件过滤器
   * 过滤掉不需要合并的文件
   */
  public static class MergeFileFilter extends Configured implements PathFilter {

    //过滤出小文件,去掉不必统计的目录
    @Override
    public boolean accept(Path path) {
      //过滤不统计的文件和目录
      Configuration conf = getConf();
      if (conf == null) {
        conf = new Configuration();
      }
      FileSystem fs = null;
      try {
        fs = path.getFileSystem(conf);
        boolean isTrash = path.toString().contains(".Trash");
        boolean isDataExport = path.toString().contains("/data_export.db/");
        boolean isTmp = path.getName().startsWith(".") || path.getName().startsWith("_");
        boolean isSmallFile = fs.getFileStatus(path).getLen() < Config.getMergeLessThanSize(); // 获取小文件M
        String base = conf.get(Config.INPUT_DIR);
        boolean isbase = false;
        boolean isDir = fs.isDirectory(path);
        if (!StringUtils.isEmpty(base)) {
          isbase = path.equals(fs.resolvePath(new Path(base)));
        }
        return !(isTrash || isDataExport || isTmp) && !(isDir && !isbase)
                && !path.getName().endsWith(".lzo.index") && isSmallFile;
      } catch (IOException e) {
      }
      return false;
    }
  }

  /**
   * 小文件目录过滤器
   * 过滤掉不需要合并的目录
   */
  public static class MergeDirFilter implements PathFilter {
    //过滤出小文件,去掉不必统计的目录
    @Override
    public boolean accept(Path path) {
      //过滤不统计的文件和目录
      Configuration conf = new Configuration();
      FileSystem fs = null;
      try {
        fs = path.getFileSystem(conf);
        boolean isTrash = path.toString().contains(".Trash");
        boolean isDataExport = path.toString().contains("/data_export.db/");
        boolean isTmp = path.getName().startsWith(".") || path.getName().startsWith("_");
        boolean isDir = fs.isDirectory(path);
        return !(isTrash || isDataExport || isTmp) && isDir;
      } catch (IOException e) {
      }
      return false;
    }
  }
}
