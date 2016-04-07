package com.jd.bdp.hdfs.mergefiles.mapreduce.lib;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import java.io.IOException;

/**
 * Created by wubiao on 1/24/16.
 */
public class CompressedCombineFileInputFormat
        extends CombineFileInputFormat<CompressedCombineFileWritable, Text> {

  public CompressedCombineFileInputFormat() {
    super();

  }

  public RecordReader<CompressedCombineFileWritable, Text>
  createRecordReader(InputSplit split,
                     TaskAttemptContext context) throws IOException {
    return new
            CombineFileRecordReader<CompressedCombineFileWritable,
                    Text>((CombineFileSplit) split, context,
            CompressedCombineFileRecordReader.class);
  }

  @Override
  protected boolean isSplitable(JobContext context, Path file) {
    return false;
  }

}
