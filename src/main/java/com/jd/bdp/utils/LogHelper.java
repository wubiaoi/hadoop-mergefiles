package com.jd.bdp.utils;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;

import java.io.PrintStream;

/**
 * Created by wubiao on 1/21/16.
 */
public class LogHelper {

  protected Log LOG;
  protected boolean isSilent;

  public LogHelper(Log LOG) {
    this(LOG, false);
  }

  public LogHelper(Log LOG, boolean isSilent) {
    this.LOG = LOG;
    this.isSilent = isSilent;
  }

  public PrintStream getOutStream() {
    return System.out;
  }

  public static PrintStream getInfoStream() {
    return getErrStream();
  }

  public static PrintStream getErrStream() {
    return System.err;
  }

  public PrintStream getChildOutStream() {
    return System.out;
  }

  public PrintStream getChildErrStream() {
    return System.err;
  }

  public boolean getIsSilent() {
    return isSilent;
  }

  public void logInfo(String info) {
    logInfo(info, null);
  }

  public void logInfo(String info, String detail) {
    LOG.info(info + StringUtils.defaultString(detail));
  }

  public void printInfo(String info) {
    printInfo(info, null);
  }

  public void printInfo(String info, String detail) {
    if (!getIsSilent()) {
      getInfoStream().println(info);
    }
    LOG.info(info + StringUtils.defaultString(detail));
  }

  public void printError(String error) {
    printError(error, null);
  }

  public void printError(String error, String detail) {
    getErrStream().println(error);
    LOG.error(error + StringUtils.defaultString(detail));
  }
}