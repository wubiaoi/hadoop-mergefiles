package com.jd.bdp.hdfs.mergefiles.mapreduce;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.*;
import org.apache.hadoop.hive.shims.CombineHiveKey;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * 合并ORC文件Mapper
 * Created by wubiao on 1/22/16.
 */
public class OrcMergeFilesMapper extends MapReduceBase implements Mapper {

  // These parameters must match for all orc files involved in merging. If it
  // does not merge, the file will be put into incompatible file set and will
  // not be merged.
  CompressionKind compression = null;
  long compressBuffSize = 0;
  OrcFile.Version version;
  int columnCount = 0;
  int rowIndexStride = 0;

  Writer outWriter;
  Path prevPath;
  private Reader reader;
  private FSDataInputStream fdis;

  protected Set<Path> incompatFileSet;
  private FileSystem fs;
  private JobConf jobConf;
  private Path outPath;

  private final static Log LOG = LogFactory.getLog(OrcMergeFilesMapper.class);

  @Override
  public void configure(JobConf job) {
    incompatFileSet = new HashSet<Path>();
    jobConf = job;
    outPath = FileOutputFormat.getOutputPath(job);
    LOG.info("Output Path:" + outPath);
    try {
      fs = FileInputFormat.getInputPaths(job)[0].getFileSystem(job);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void map(Object key, Object value, OutputCollector outputCollector, Reporter reporter) throws IOException {
    process(key, value, reporter);
  }

  @Override
  public void close() throws IOException {
    super.close();
    if (outWriter != null) {
      outWriter.close();
    }
  }

  private void process(Object key, Object value, Reporter reporter)
          throws IOException {
    try {
      OrcFileValueWrapper v;
      OrcFileKeyWrapper k;
      if (key instanceof CombineHiveKey) {
        k = (OrcFileKeyWrapper) ((CombineHiveKey) key).getKey();
      } else {
        k = (OrcFileKeyWrapper) key;
      }

      // skip incompatible file, files that are missing stripe statistics are set to incompatible
      if (k.isIncompatFile()) {
        LOG.warn("Incompatible ORC file merge! Stripe statistics is missing. " + k.getInputPath());
        incompatFileSet.add(k.getInputPath());
        return;
      }

      v = (OrcFileValueWrapper) value;

      if (prevPath == null) {
        prevPath = k.getInputPath();
        reader = OrcFile.createReader(fs, k.getInputPath());
      }

      // store the orc configuration from the first file. All other files should
      // match this configuration before merging else will not be merged
      if (outWriter == null) {
        compression = k.getCompression();
        compressBuffSize = k.getCompressBufferSize();
        version = k.getVersion();
        columnCount = k.getTypes().get(0).getSubtypesCount();
        rowIndexStride = k.getRowIndexStride();
        Path file = new Path(outPath, k.getInputPath().getName());
        // block size and stripe size will be from config
        outWriter = OrcFile.createWriter(file,
                OrcFile.writerOptions(jobConf)
                        .compress(compression)
                        .version(version)
                        .rowIndexStride(rowIndexStride)
                        .inspector(reader.getObjectInspector()));
      }

      if (!checkCompatibility(k)) {
        incompatFileSet.add(k.getInputPath());
        return;
      }

      // next file in the path
      if (!k.getInputPath().equals(prevPath)) {
        reader = OrcFile.createReader(fs, k.getInputPath());
      }

      // initialize buffer to read the entire stripe
      byte[] buffer = new byte[(int) v.getStripeInformation().getLength()];
      fdis = fs.open(k.getInputPath());
      fdis.readFully(v.getStripeInformation().getOffset(), buffer, 0,
              (int) v.getStripeInformation().getLength());

      // append the stripe buffer to the new ORC file
      outWriter.appendStripe(buffer, 0, buffer.length, v.getStripeInformation(),
              v.getStripeStatistics());
      // update map input counter
      reporter.getCounter("org.apache.hadoop.mapreduce.TaskCounter", "MAP_INPUT_RECORDS").increment(v.getStripeStatistics().getColStats(0).getNumberOfValues() - 1); //去掉本身文件数的增长
      reporter.getCounter("org.apache.hadoop.mapreduce.TaskCounter", "MAP_OUTPUT_RECORDS").increment(v.getStripeStatistics().getColStats(0).getNumberOfValues());

      LOG.info("Merged stripe from file " + k.getInputPath() + " [ offset : "
              + v.getStripeInformation().getOffset() + " length: "
              + v.getStripeInformation().getLength() + " row: "
              + v.getStripeStatistics().getColStats(0).getNumberOfValues() + " ]");

      // add user metadata to footer in case of any
      if (v.isLastStripeInFile()) {
        outWriter.appendUserMetadata(v.getUserMetadata());
      }
    } catch (Throwable e) {
      LOG.error("Closing operator..Exception: " + ExceptionUtils.getStackTrace(e));
      throw new IOException(e);
    }
  }

  private boolean checkCompatibility(OrcFileKeyWrapper k) {
    // check compatibility with subsequent files
    if ((k.getTypes().get(0).getSubtypesCount() != columnCount)) {
      LOG.warn("Incompatible ORC file merge! Column counts mismatch for " + k.getInputPath());
      return false;
    }

    if (!k.getCompression().equals(compression)) {
      LOG.warn("Incompatible ORC file merge! Compression codec mismatch for " + k.getInputPath());
      return false;
    }

    if (k.getCompressBufferSize() != compressBuffSize) {
      LOG.warn("Incompatible ORC file merge! Compression buffer size mismatch for " + k.getInputPath());
      return false;

    }

    if (!k.getVersion().equals(version)) {
      LOG.warn("Incompatible ORC file merge! Version mismatch for " + k.getInputPath());
      return false;
    }

    if (k.getRowIndexStride() != rowIndexStride) {
      LOG.warn("Incompatible ORC file merge! Row index stride mismatch for " + k.getInputPath());
      return false;
    }

    return true;
  }

}
