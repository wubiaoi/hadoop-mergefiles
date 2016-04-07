package com.jd.bdp.hdfs.mergefiles.mapreduce.lib;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.merge.MergeFileInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcFileKeyWrapper;
import org.apache.hadoop.hive.ql.io.orc.OrcFileStripeMergeRecordReader;
import org.apache.hadoop.hive.ql.io.orc.OrcFileValueWrapper;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

/**
 * Created by wubiao on 3/25/16.
 */
public class OrcFileStripeMergeInputFormat extends MergeFileInputFormat {

  @Override
  public RecordReader<OrcFileKeyWrapper, OrcFileValueWrapper> getRecordReader(
          InputSplit split, JobConf job, Reporter reporter) throws IOException {

    reporter.setStatus(split.toString());
    return new OrcFileStripeMergeRecordReader(job, (FileSplit) split);
  }

  @Override
  protected boolean isSplitable(FileSystem fs, Path filename) {
    return false;
  }
}
