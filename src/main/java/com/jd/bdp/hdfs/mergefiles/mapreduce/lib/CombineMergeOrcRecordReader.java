package com.jd.bdp.hdfs.mergefiles.mapreduce.lib;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.orc.OrcFileStripeMergeRecordReader;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.CombineFileSplit;

import java.io.IOException;

/**
 * Created by wubiao on 4/6/16.
 */
public class CombineMergeOrcRecordReader<K, V> implements RecordReader<K, V> {

  protected RecordReader recordReader;

  public CombineMergeOrcRecordReader() {
  }

  public CombineMergeOrcRecordReader(CombineFileSplit split,
                                     Configuration conf, Reporter reporter, Integer index) throws IOException {
    FileSplit fsplit = new FileSplit(split.getPaths()[index], split
            .getStartOffsets()[index], split.getLengths()[index], split
            .getLocations());
    recordReader = new OrcFileStripeMergeRecordReader(conf, fsplit);
  }

  @Override
  public boolean next(K k, V v) throws IOException {
    return recordReader.next(k, v);
  }

  @Override
  public K createKey() {
    return (K) recordReader.createKey();
  }

  @Override
  public V createValue() {
    return (V) recordReader.createValue();
  }

  @Override
  public long getPos() throws IOException {
    return recordReader.getPos();
  }

  @Override
  public void close() throws IOException {
    recordReader.close();
  }

  @Override
  public float getProgress() throws IOException {
    return recordReader.getProgress();
  }
}
