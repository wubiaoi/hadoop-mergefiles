package com.jd.bdp.hdfs.mergefiles.mapreduce.lib;

import org.apache.hadoop.hive.ql.io.orc.OrcFileKeyWrapper;
import org.apache.hadoop.hive.ql.io.orc.OrcFileValueWrapper;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.CombineFileInputFormat;
import org.apache.hadoop.mapred.lib.CombineFileRecordReader;
import org.apache.hadoop.mapred.lib.CombineFileSplit;

import java.io.IOException;

/**
 * Created by wubiao on 3/28/16.
 */
public class CombineMergeOrcInputFormat extends CombineFileInputFormat {

  public CombineMergeOrcInputFormat() {
  }

  @Override
  public RecordReader<OrcFileKeyWrapper, OrcFileValueWrapper> getRecordReader(InputSplit inputSplit,
                                      JobConf jobConf, Reporter reporter) throws IOException {
    CombineFileSplit cfSplit = (CombineFileSplit) inputSplit;
    return new CombineFileRecordReader(jobConf, cfSplit, reporter, CombineMergeOrcRecordReader.class);
  }

}
