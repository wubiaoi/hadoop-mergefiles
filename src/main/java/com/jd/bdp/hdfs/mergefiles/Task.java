package com.jd.bdp.hdfs.mergefiles;

/**
 * Created by wubiao on 1/21/16.
 */
public interface Task {

  void init(String[] args) throws Exception;

  int run() throws Exception;
}
