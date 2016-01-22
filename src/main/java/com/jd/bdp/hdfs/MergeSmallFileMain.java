package com.jd.bdp.hdfs;

import com.jd.bdp.hdfs.mergefiles.*;
import com.jd.bdp.utils.Utils;
import com.jd.bdp.hdfs.mergefiles.mr.MergePath;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MD5Hash;

/**
 * Created by wubiao on 1/21/16.
 */
public class MergeSmallFileMain {
  public static void main(String[] args) throws Exception {
    Task mergeTask = new MergeTask();
    mergeTask.init(args);
    mergeTask.run();
  }
}
