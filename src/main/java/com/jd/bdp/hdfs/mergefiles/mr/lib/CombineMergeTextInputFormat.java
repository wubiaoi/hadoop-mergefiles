package com.jd.bdp.hdfs.mergefiles.mr.lib;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import java.io.IOException;

/**
 * Text格式合并InputFormat
 * Created by wubiao on 1/21/16.
 */
public class CombineMergeTextInputFormat extends CombineFileInputFormat<LongWritable, Text> {

  @Override
  public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {

    CombineFileRecordReader<LongWritable, Text> reader =
            new CombineFileRecordReader<LongWritable, Text>(
                    (CombineFileSplit) split, context, CombineMergeTextRecordReader.class);
    return reader;
  }

}
