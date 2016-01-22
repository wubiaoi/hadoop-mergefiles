package com.jd.bdp.hdfs.mergefiles.mr;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by wubiao on 1/22/16.
 */
public class MergeFormatMapper extends Mapper<NullWritable, Text, Text, NullWritable> {
  @Override
  protected void map(NullWritable key, Text value, Context context) throws IOException, InterruptedException {
    context.write(new Text(value), NullWritable.get());
  }
}
