package com.jd.bdp.hdfs.mergefiles.mr.lib;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;

/**
 * Text格式InputFormat对应的RecordReader
 * Created by wubiao on 1/21/16.
 */
public class CombineMergeTextRecordReader extends RecordReader<LongWritable, Text> {

  private LineRecordReader lineRecordReader = new LineRecordReader();

  public CombineMergeTextRecordReader(CombineFileSplit split, TaskAttemptContext context, Integer index) throws IOException {
    FileSplit fileSplit = new FileSplit(split.getPath(index), split.getOffset(index), split.getLength(index), split.getLocations());
    lineRecordReader.initialize(fileSplit, context);
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
  }

  @Override
  public LongWritable getCurrentKey() throws IOException, InterruptedException {
    return lineRecordReader.getCurrentKey();
  }

  @Override
  public Text getCurrentValue() throws IOException, InterruptedException {
    return lineRecordReader.getCurrentValue();
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    return lineRecordReader.nextKeyValue();
  }

  @Override
  public float getProgress() throws IOException {
    return lineRecordReader.getProgress();
  }

  @Override
  public void close() throws IOException {
    lineRecordReader.close();
  }
}
