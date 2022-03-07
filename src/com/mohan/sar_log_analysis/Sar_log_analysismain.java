package com.mohan.sar_log_analysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
public class Sar_log_analysismain
{
  public static void main(String[] args) throws Exception
  {
    Configuration conf = new Configuration();
    String[] pathArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (pathArgs.length < 2)
    {
      System.err.println("MR Project Usage: sarlog <input-path> [...] <output-path>");
      System.exit(2);
    }
    Job job = Job.getInstance(conf, "MapReduce cpu-utilization");
    job.setJarByClass(Sar_log_analysismain.class);
    job.setMapperClass(Sar_log_analysisMapper.class);
    job.setReducerClass(Sar_log_analysisReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(FloatWritable.class);
	
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    for (int i = 0; i < pathArgs.length - 1; ++i)
    {
      FileInputFormat.addInputPath(job, new Path(pathArgs[i]));
    }
    
	FileOutputFormat.setOutputPath(job, new Path(pathArgs[pathArgs.length-1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}