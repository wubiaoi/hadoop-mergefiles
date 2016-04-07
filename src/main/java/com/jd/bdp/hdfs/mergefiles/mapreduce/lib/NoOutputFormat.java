package com.jd.bdp.hdfs.mergefiles.mapreduce.lib;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;

/**
 * Created by wubiao on 3/25/16.
 */
public class NoOutputFormat<K extends WritableComparable<K>, V extends Writable>
        implements OutputFormat<K, V> {
  //no records will be emited from Hive
  @Override
  public RecordWriter<K, V> getRecordWriter(FileSystem ignored, JobConf job, String name,
                                            Progressable progress) {
    return new RecordWriter<K, V>() {
      public void write(K key, V value) {
        throw new RuntimeException("Should not be called");
      }

      public void close(Reporter reporter) {
      }
    };
  }

  @Override
  public void checkOutputSpecs(FileSystem fileSystem, JobConf jobConf) throws IOException {

  }
}
