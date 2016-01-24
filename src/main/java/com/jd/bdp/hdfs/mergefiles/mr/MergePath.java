package com.jd.bdp.hdfs.mergefiles.mr;

import com.jd.bdp.hdfs.mergefiles.FileType;
import org.apache.hadoop.fs.Path;

/**
 * 包装合并的path,方便获取信息
 * Created by wubiao on 1/20/16.
 */
public class MergePath {

  private Path path;
  private FileType type;
  private long size;
  private long numFiles;
  private Path tmpDir;
  private Path logDir;
  private long mapInput;
  private long mapOutput;

  public MergePath() {
  }

  public MergePath(Path path, FileType type, long size, long numFiles) {
    this.path = path;
    this.type = type;
    this.size = size;
    this.numFiles = numFiles;
  }

  public long getMapOutput() {
    return mapOutput;
  }

  public void setMapOutput(long mapOutput) {
    this.mapOutput = mapOutput;
  }

  public long getMapInput() {
    return mapInput;
  }

  public void setMapInput(long mapInput) {
    this.mapInput = mapInput;
  }

  public Path getTmpDir() {
    return tmpDir;
  }

  public void setTmpDir(Path tmpDir) {
    this.tmpDir = tmpDir;
  }

  public Path getLogDir() {
    return logDir;
  }

  public void setLogDir(Path logDir) {
    this.logDir = logDir;
  }

  public Path getPath() {
    return path;
  }

  public void setPath(Path path) {
    this.path = path;
  }

  public FileType getType() {
    return type;
  }

  public void setType(FileType type) {
    this.type = type;
  }

  public long getSize() {
    return size;
  }

  public void setSize(long size) {
    this.size = size;
  }

  public long getNumFiles() {
    return numFiles;
  }

  public void setNumFiles(long numFiles) {
    this.numFiles = numFiles;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    MergePath mergePath = (MergePath) o;

    if (!path.equals(mergePath.path)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    return path.hashCode();
  }
}
