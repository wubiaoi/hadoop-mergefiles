package com.jd.bdp.hdfs;

import com.jd.bdp.hdfs.mergefiles.*;

/**
 * Created by wubiao on 1/21/16.
 */
public class MergeSmallFileMain {
  public static void main(String[] args) throws Exception {
    Task mergeTask = new MergeTask();
    mergeTask.init(args);
    mergeTask.run();
    mergeTask.close();
  }
}
