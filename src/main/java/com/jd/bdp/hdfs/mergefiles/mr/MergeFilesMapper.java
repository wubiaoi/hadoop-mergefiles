package com.jd.bdp.hdfs.mergefiles.mr;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 合并文件Mapper
 * Created by wubiao on 1/20/16.
 */
public class MergeFilesMapper extends Mapper<Object, Text, Text, NullWritable> {
  @Override
  protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    context.write(new Text(value), NullWritable.get());
  }
}

