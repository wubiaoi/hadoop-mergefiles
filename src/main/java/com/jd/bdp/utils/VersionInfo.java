package com.jd.bdp.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Created by wubiao on 2/18/16.
 */
public class VersionInfo {
  private final static Log log = LogFactory.getLog(VersionInfo.class);
  public final static String version = "1.6";

  public static void main(String[] args) {
    LogHelper console = new LogHelper(log);
    console.printError("dsfadsafsdafs");
  }
}
