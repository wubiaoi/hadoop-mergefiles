package com.jd.bdp.hdfs.mergefiles;

import com.hadoop.compression.lzo.DistributedLzoIndexer;
import com.hadoop.compression.lzo.LzopCodec;
import com.hadoop.mapreduce.LzoTextInputFormat;
import com.jd.bdp.hdfs.mergefiles.mr.MergePath;
import com.jd.bdp.hdfs.mergefiles.mr.lib.CombineMergeTextInputFormat;
import com.jd.bdp.hdfs.mergefiles.mr.lib.Filter;
import com.jd.bdp.hdfs.mergefiles.mr.MergeFilesMapper;
import com.jd.bdp.utils.LogHelper;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobPriority;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import java.util.concurrent.atomic.AtomicLong;

/**
 * TaskRunner implementation.
 */

public class TaskRunner extends Thread {

  private final static Log log = LogFactory.getLog(TaskRunner.class);
  private final LogHelper console;
  private final String PREFIX;

  protected Job job;
  private MergePath mergePath;
  private Path input;
  private Path output;
  private Path targetPath;
  private FileType inputType;
  private FileType outputType;
  protected int result;
  protected Exception exception;
  protected boolean isRunning;
  private JobStatus.State jobstatus;
  private MergeContext context;
  private static AtomicLong taskCounter = new AtomicLong(0);
  private static ThreadLocal<Long> taskRunnerID = new ThreadLocal<Long>() {
    @Override
    protected Long initialValue() {
      return taskCounter.incrementAndGet();
    }
  };

  protected Thread runner;

  public TaskRunner(MergePath mergePath, MergeContext context) {
    this.mergePath = mergePath;
    this.input = mergePath.getPath();
    this.output = new Path(Config.getTmpDir(), MD5Hash.digest(input.toString()).toString());
    this.inputType = mergePath.getType();
    this.outputType = mergePath.getType();
    this.context = context;
    this.console = new LogHelper(log, true);
    this.PREFIX = "[" + input + "]Merge Task===>";
  }

  public String getPREFIX() {
    return PREFIX;
  }

  public MergePath getMergePath() {
    return mergePath;
  }

  public Path getTargetPath() {
    return targetPath;
  }

  public void setTargetPath(Path targetPath) {
    this.targetPath = targetPath;
  }

  public FileType getInputType() {
    return inputType;
  }

  public void setInputType(FileType inputType) {
    this.inputType = inputType;
  }

  public FileType getOutputType() {
    return outputType;
  }

  public void setOutputType(FileType outputType) {
    this.outputType = outputType;
  }

  public Exception getException() {
    return exception;
  }

  public void setException(Exception exception) {
    this.exception = exception;
  }

  public Path getInput() {
    return input;
  }

  public void setInput(Path input) {
    this.input = input;
  }

  public Path getOutput() {
    return output;
  }

  public void setOutput(Path output) {
    this.output = output;
  }

  public JobStatus.State getJobstatus() {
    return jobstatus;
  }

  public void setJobstatus(JobStatus.State jobstatus) {
    this.jobstatus = jobstatus;
  }

  public Job getJob() {
    return job;
  }

  public void setJob(Job job) {
    this.job = job;
  }

  public int getResult() {
    return result;
  }

  public void setResult(int result) {
    this.result = result;
  }

  public boolean isRunning() {
    return isRunning;
  }

  public void setRunning(boolean isRunning) {
    this.isRunning = isRunning;
  }

  @Override
  public void run() {
    runner = Thread.currentThread();
    context.launching(this);
    setRunning(true);
    try {
      runMergeJob(input, output);
    } finally {
      runner = null;
      setRunning(false);
    }
  }

  /**
   * 启动合并job
   */

  public int runMergeJob(Path input, Path output) {
    Configuration conf = new Configuration();
    try {
      console.printInfo("Start Merge: " + input + " ,Type:" + inputType + ",Hash:[" +
              MD5Hash.digest(input.toString()).toString() + "]");
      FileSystem fs = FileSystem.get(conf);
      conf.set("mapreduce.job.priority", JobPriority.VERY_HIGH.name());
      conf.set("mapred.job.priority", JobPriority.VERY_HIGH.name());

      Job job = Job.getInstance(conf, "MergeFiles:" + input);
      FileInputFormat.setInputPathFilter(job, Filter.MergeFileFilter.class);
      job.setJarByClass(TaskRunner.class);

      //根据文件类型选择输入输出类型
      if (inputType.equals(FileType.TEXT)) {
        job.setInputFormatClass(CombineMergeTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.getConfiguration().setLong("mapreduce.input.fileinputformat.split.minsize.per.node", Config.getMergeMaxSize());
        job.getConfiguration().setLong("mapreduce.input.fileinputformat.split.maxsize", Config.getMergeMaxSize() + 50 * 1024 * 1024);
      } else if (inputType.equals(FileType.LZO)) {
        job.setInputFormatClass(CombineMergeTextInputFormat.class);
        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, LzopCodec.class);
        job.getConfiguration().setLong("mapreduce.input.fileinputformat.split.minsize.per.node", Config.getMergeMaxSize());
        job.getConfiguration().setLong("mapreduce.input.fileinputformat.split.maxsize", Config.getMergeMaxSize() + 50 * 1024 * 1024);
      } else if (inputType.equals(FileType.ORC)) {
        throw new Exception("Not support ORC file merge now."); //todo
      } else if (inputType.equals(FileType.AVRO)) {
        job.setInputFormatClass(AvroKeyInputFormat.class);
        job.setOutputFormatClass(AvroKeyValueOutputFormat.class);//todo
        return 1;
      } else if (inputType.equals(FileType.UNKNOWN)) {
        throw new Exception("UnKnow file type.If you make sure it is a text format.add -t run this path[" + input + "] again.");
      }

      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(Text.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);

      job.setMapperClass(MergeFilesMapper.class);
      job.setNumReduceTasks(0);

      FileInputFormat.setInputPaths(job, input);
      FileOutputFormat.setOutputPath(job, output);
      int res = job.waitForCompletion(true) ? 0 : 1;
      setJobstatus(job.getJobState());
      if (res == 0 && jobstatus.equals(JobStatus.State.SUCCEEDED)) {
        console.printInfo(this.PREFIX + "Merge Job finished successfully");
        //创建索引
        int exitCode = 1;
        if (inputType.equals(FileType.LZO)) {
          console.printInfo(PREFIX + "Start build Lzo Index...");
          exitCode = ToolRunner.run(new DistributedLzoIndexer(), new String[]{output.toString()});
          if (exitCode != 0) {
            console.printError(PREFIX + "build Lzo Index Failed");
            throw new Exception(PREFIX + "build Lzo Index Failed,exitCode:" + exitCode);
          }
        }
        // 开始moveTask
        if (Config.getMergeTargePath() != null) {
          targetPath = Config.getMergeTargePath();
        } else {
          targetPath = input;
        }
        //备份原数据到tmpDir/outpath/old目录
        Path oldData = new Path(output, ".old");
        fs.mkdirs(oldData);
        FileStatus[] data = fs.listStatus(input, new Filter.MergeFileFilter());
        Path moveLog = new Path(oldData, ".backup.log");
        Path mergeLog = new Path(oldData, ".merge.log");
        console.printInfo(this.PREFIX + "Start move file:" + input + " to " + oldData);
        FSDataOutputStream out = fs.create(moveLog);
        for (FileStatus f : data) {
          if (f.isDirectory()) continue;
          fs.rename(f.getPath(), oldData);
          //只获取小文件的index文件
          if (inputType.equals(FileType.LZO)) {
            Path lzoIndex = new Path(f.getPath().getParent(), new Path(f.getPath().getName() + ".index"));
            try {
              fs.rename(lzoIndex, oldData);
              out.writeBytes(lzoIndex + "\n");
            } catch (Exception e) {
            }
          }
          out.writeBytes(f.getPath() + "\n");
        }
        data = null;
        out.close();
        console.printInfo(this.PREFIX + "move file:" + input + " to " + oldData + " success");
        console.printInfo(this.PREFIX + "Start move file:" + output + " to " + targetPath);
        data = fs.listStatus(output, new PathFilter() {
          @Override
          public boolean accept(Path path) {
            return !path.getName().startsWith(".") && !path.getName().startsWith("_");
          }
        });
        out = fs.create(mergeLog);
        for (FileStatus f : data) {
          if (f.isDirectory()) continue;
          Path newPath = new Path(targetPath, "merge_" + f.getPath().getName());
          fs.rename(f.getPath(), newPath);
          out.writeBytes(f.getPath() + "\n");
        }
        console.printInfo(this.PREFIX + "move file " + output + " to " + targetPath);
        out.close();
      }
      return res;
    } catch (Exception e) {
      exception = e;
      return 200;
    }
  }

  public static long getTaskRunnerID() {
    return taskRunnerID.get();
  }

}
