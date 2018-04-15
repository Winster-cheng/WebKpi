package com.cpl.kpi.ips;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by Winstercheng on 2016/11/03 ips和pv的区别在于ips每个IP只计算一次请求
 */

public class IpsDriver extends Configured  {

	public static void main(String[] args) throws Exception {
			Job job1=getJob1();
			Job job2=SecondReduce.getJob2();
	        ControlledJob controlledJob1 = new ControlledJob(job1.getConfiguration());  
	        controlledJob1.setJob(job1);  
	          
	        ControlledJob controlledJob2 = new ControlledJob(job2.getConfiguration());  
	        controlledJob2.setJob(job2);  
	        controlledJob2.addDependingJob(controlledJob1);  
	          
	        JobControl jc = new JobControl("ips job control");  
	        jc.addJob(controlledJob1);  
	        jc.addJob(controlledJob2);  
	        Thread jcThread = new Thread(jc);  
	        jcThread.start();  
	        while(true){  
	            if(jc.allFinished()){  
	                System.out.println(jc.getSuccessfulJobList());  
	                jc.stop();  
	                break;
	            }  
	            if(jc.getFailedJobList().size() > 0){  
	                System.out.println(jc.getFailedJobList());  
	                jc.stop();  
	                break;
	            }  
	        }  
	}

	public static Job getJob1() throws Exception {
		Configuration conf = new Configuration();
		String input = "/user/data/kpi/access.20120104.log";
		String output = "/user/data/kpi/ips/out";
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path(output))) {
			fs.delete(new Path(output), true);
		}
		Job job1 = Job.getInstance(conf, "ips");
		job1.setJarByClass(IpsDriver.class);

		job1.setMapperClass(Mapper.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);

		job1.setReducerClass(Reducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);

		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);

		TextInputFormat.addInputPath(job1, new Path(input));
		TextOutputFormat.setOutputPath(job1, new Path(output));
		return job1;
	}
}
