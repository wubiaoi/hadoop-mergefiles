package com.jd.bdp.hdfs.mergefiles;

import com.jd.bdp.hdfs.mergefiles.mapreduce.MergePath;

import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by wubiao on 1/20/16.
 */
public class MergeContext {
  private Queue<MergePath> runnable;
  private Queue<TaskRunner> running;

  public MergeContext() {
    this.runnable = new ConcurrentLinkedQueue<MergePath>();
    this.running = new LinkedBlockingQueue<TaskRunner>();
  }

  public synchronized int getTotal() {
    return runnable.size() + running.size();
  }

  public synchronized boolean isRunning() {
    return !running.isEmpty() || !runnable.isEmpty();
  }

  public synchronized void launching(TaskRunner runner) {
    running.add(runner);
  }

  public synchronized boolean addToRunnable(MergePath path) {
    if (runnable.contains(path)) {
      return false;
    }
    runnable.add(path);
    return true;
  }

  public synchronized MergePath getRunnable(int maxJob) {
    if (runnable.peek() != null && running.size() < maxJob) {
      return runnable.remove();
    }
    return null;
  }

  public synchronized TaskRunner pollFinished() throws InterruptedException {
    while (true) {
      Iterator<TaskRunner> it = running.iterator();
      while (it.hasNext()) {
        TaskRunner runner = it.next();
        if (runner != null && !runner.isRunning()) {
          it.remove();
          return runner;
        }
      }
      wait(2000);
    }
  }

}
