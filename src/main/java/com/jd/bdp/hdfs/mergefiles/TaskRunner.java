package com.jd.bdp.hdfs.mergefiles;

import com.hadoop.compression.lzo.DistributedLzoIndexer;
import com.hadoop.compression.lzo.LzopCodec;
import com.jd.bdp.hdfs.mergefiles.exception.UnsupportedTypeException;
import com.jd.bdp.hdfs.mergefiles.mapreduce.MergeFilesMapper;
import com.jd.bdp.hdfs.mergefiles.mapreduce.MergePath;
import com.jd.bdp.hdfs.mergefiles.mapreduce.OrcMergeFilesMapper;
import com.jd.bdp.hdfs.mergefiles.mapreduce.lib.*;
import com.jd.bdp.utils.LogHelper;
import com.jd.bdp.utils.Utils;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobPriority;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * TaskRunner implementation.
 */

public class TaskRunner extends Thread {

  private final static Log log = LogFactory.getLog(TaskRunner.class);
  private final LogHelper console;
  private final String PREFIX;

  protected Job job;
  private String jobId;
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
  private Configuration conf;
  private FileSystem fs;
  private FSDataOutputStream out;
  private static AtomicLong taskCounter = new AtomicLong(0);
  private static ThreadLocal<Long> taskRunnerID = new ThreadLocal<Long>() {
    @Override
    protected Long initialValue() {
      return taskCounter.incrementAndGet();
    }
  };

  protected Thread runner;

  public TaskRunner(MergePath mergePath, MergeContext context) throws IOException {
    setRunning(true);
    this.mergePath = mergePath;
    this.input = mergePath.getPath();
    this.output = new Path(mergePath.getTmpDir().getParent(), mergePath.getTmpDir().getName() + "_merge");
    this.inputType = mergePath.getType();
    this.outputType = mergePath.getType();
    this.context = context;
    this.console = new LogHelper(log, true);
    this.PREFIX = "[" + input + "]Merge Task===>";
    this.conf = new Configuration();
    this.fs = FileSystem.get(conf);
    this.out = fs.create(new Path(mergePath.getLogDir(), "error.log"));
    if (Config.getMergeTargePath() != null) {
      targetPath = new Path(Utils.cutSuffix(Config.getMergeTargePath().toString(), "/") + Path.getPathWithoutSchemeAndAuthority(input));
      fs.mkdirs(targetPath);
    } else {
      targetPath = input;
    }
  }

  public String getJobId() {
    return jobId;
  }

  public void setJobId(String jobId) {
    this.jobId = jobId;
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
    try {
      result = runMergeJob(input, output);
    } finally {
      runner = null;
      setRunning(false);
    }
  }

  /**
   * 启动合并job
   */

  public int runMergeJob(Path input, Path output) {
    try {
      console.printInfo("Start Merge(" + getTaskRunnerID() + "/" + context.getTotal() + "): " + input + " ,Type:" + inputType);
      conf.set("mapreduce.job.priority", JobPriority.VERY_HIGH.name());
      conf.set("mapred.job.priority", JobPriority.VERY_HIGH.name());
      conf.setInt("mapreduce.job.max.split.locations", Integer.MAX_VALUE);
      conf.setBoolean("mapreduce.map.speculative", false); //关闭推测执行,防止写文件冲突
      conf.setLong("mapreduce.input.fileinputformat.split.minsize.per.node", Config.getMergeMaxSize());
      conf.setLong("mapreduce.input.fileinputformat.split.maxsize", Config.getMergeMaxSize());
      conf.setLong("mapreduce.input.fileinputformat.split.minsize.per.rack", Config.getMergeMaxSize());


      Job job = null;
      JobConf jobconf = null;
      if (isOldMR(inputType)) {
        jobconf = new JobConf(conf, TaskRunner.class);
        jobconf.setJobName("MergeFiles:" + input);
      } else {
        job = Job.getInstance(conf, "MergeFiles:" + input);
        job.setJarByClass(TaskRunner.class);
      }

      //根据文件类型选择输入输出类型
      if (inputType.equals(FileType.TEXT)) {
        job.setInputFormatClass(CombineMergeTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
      } else if (inputType.equals(FileType.LZO)) {
        job.setInputFormatClass(CompressedCombineFileInputFormat.class);
        conf.set("io.compression.codecs", "com.hadoop.compression.lzo.LzopCodec");
        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, LzopCodec.class);
      } else if (inputType.equals(FileType.ORC)) {
        jobconf.setInputFormat(CombineMergeOrcInputFormat.class);
        jobconf.setOutputFormat(NoOutputFormat.class);
      } else if (inputType.equals(FileType.AVRO)) {
        job.setInputFormatClass(AvroKeyInputFormat.class);
        job.setOutputFormatClass(AvroKeyValueOutputFormat.class);//todo
        throw new UnsupportedTypeException();
      } else if (inputType.equals(FileType.UNKNOWN)) {
        throw new UnsupportedTypeException("UnKnow file type.If you make sure it is a text format.add -t run this path[" + input + "] again.");
      }

      int res = 0;
      Counter mapInput;
      Counter mapOutput;
      if (isOldMR(inputType)) {
        jobconf.setMapOutputValueClass(NullWritable.class);
        jobconf.setOutputValueClass(NullWritable.class);
        jobconf.setMapperClass(OrcMergeFilesMapper.class);
        jobconf.setMapOutputKeyClass(NullWritable.class);
        jobconf.setOutputKeyClass(NullWritable.class);
        jobconf.setNumReduceTasks(0);
        OrcFileStripeMergeInputFormat.setInputPaths(jobconf, input);
        OrcOutputFormat.setOutputPath(jobconf, output);

        RunningJob rj = JobClient.runJob(jobconf);
        setJobId(rj.getID().toString());
        mapInput = rj.getCounters().findCounter(("org.apache.hadoop.mapreduce.TaskCounter"), "MAP_INPUT_RECORDS");
        mapOutput = rj.getCounters().findCounter(("org.apache.hadoop.mapreduce.TaskCounter"), "MAP_OUTPUT_RECORDS");
        setJobstatus(rj.getJobStatus().getState());
      } else {
        job.setMapOutputValueClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(MergeFilesMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setOutputKeyClass(NullWritable.class);

        job.setNumReduceTasks(0);

        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output);

        res = job.waitForCompletion(true) ? 0 : 1;
        setJobId(job.getJobID().toString());
        // 开始moveTask
        mapInput = job.getCounters().findCounter(("org.apache.hadoop.mapreduce.TaskCounter"), "MAP_INPUT_RECORDS");
        mapOutput = job.getCounters().findCounter(("org.apache.hadoop.mapreduce.TaskCounter"), "MAP_OUTPUT_RECORDS");
        setJobstatus(job.getJobState());
      }


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
        if (mapInput != null) {
          mergePath.setMapInput(mapInput.getValue());
        }
        if (mapOutput != null) {
          mergePath.setMapOutput(mapOutput.getValue());
        }
        if (mergePath.getMapInput() != mergePath.getMapOutput()) {
          console.printError(this.PREFIX + " mapInput Records not equals mapOutput Records.");
          throw new Exception(this.PREFIX + " mapInput Records not equals mapOutput Records.MapInput Records:" +
                  mergePath.getMapInput() + ",MapOutputRecords:" + mergePath.getMapOutput());
        }
        //备份原数据到tmpDir目录
        Path moveLog = new Path(mergePath.getLogDir(), "mv.log");
        console.printInfo(this.PREFIX + "Start move file:" + targetPath + " to " + mergePath.getTmpDir());
        FSDataOutputStream out = fs.create(moveLog);
        out.writeBytes("JobId: " + jobId + "\n");
        out.writeBytes("move " + targetPath + " to " + mergePath.getTmpDir() + "\n");
        fs.rename(targetPath, mergePath.getTmpDir());
        console.printInfo(this.PREFIX + "move file:" + targetPath + " to " + mergePath.getTmpDir() + " success");
        console.printInfo(this.PREFIX + "Start move file:" + output + " to " + targetPath);
        fs.rename(output, targetPath);
        out.writeBytes("move " + output + " to " + targetPath + "\n");
        console.printInfo(this.PREFIX + "move file " + output + " to " + targetPath + " success");
        out.close();
      }
      return res;
    } catch (Exception e) {
      exception = e;
      return 200;
    } finally {
      try {
        fs.delete(new Path(input, ".stage"),true);
        fs.delete(new Path(getTargetPath(), "_SUCCESS"), true);
      } catch (IOException e) {
        log.warn("delete tmp .stage failed.");
        try {
          errorRecorder("delete tmp .stage Path [" + new Path(input, ".stage") + "] failed.");
        } catch (IOException e1) {
        }
      }
    }
  }

  public void errorRecorder(String msg) throws IOException {
    out.writeBytes(msg + "\n");
  }

  public void shutdown() {
    try {
      out.close();
    } catch (IOException e) {
      console.printError(ExceptionUtils.getFullStackTrace(e));
    }
  }

  public static long getTaskRunnerID() {
    return taskRunnerID.get();
  }

  public static boolean isOldMR(FileType type) {
    return type.equals(FileType.ORC);
  }

}
