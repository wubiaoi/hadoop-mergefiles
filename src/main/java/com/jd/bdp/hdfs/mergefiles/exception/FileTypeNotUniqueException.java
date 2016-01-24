package com.jd.bdp.hdfs.mergefiles.exception;

/**
 * Created by wubiao on 1/24/16.
 */
public class FileTypeNotUniqueException extends Exception {
  @Override
  public String getMessage() {
    return "The file type is not unified";
  }
}
