package com.cpl.kpi.time;

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
import com.cpl.kpi.ips.SecondReduce;
import com.cpl.kpi.time.Mapper;
import com.cpl.kpi.time.Reducer;

public class TimeDriver extends Configured {
		public static Job getJob1() throws Exception {
			Configuration conf = new Configuration();
			String input = "/user/data/kpi/access.20120104.log";
			String output = "/user/data/kpi/time/out";
			FileSystem fs = FileSystem.get(conf);
			if (fs.exists(new Path(output))) {
				fs.delete(new Path(output), true);
			}
			Job job = Job.getInstance(conf, "time");
			job.setJarByClass(TimeDriver.class);

			job.setMapperClass(Mapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);

			job.setReducerClass(Reducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);

			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			TextInputFormat.addInputPath(job, new Path(input));
			TextOutputFormat.setOutputPath(job, new Path(output));
			return job;
		}

		public static void main(String[] args) throws Exception {
			Job job1 = getJob1();
			Job job2 = SecondReduce.getJob2();
			ControlledJob controlledJob1 = new ControlledJob(
					job1.getConfiguration());
			controlledJob1.setJob(job1);

			ControlledJob controlledJob2 = new ControlledJob(
					job2.getConfiguration());
			controlledJob2.setJob(job2);
			controlledJob2.addDependingJob(controlledJob1);

			JobControl jc = new JobControl("time job control");
			jc.addJob(controlledJob1);
			jc.addJob(controlledJob2);
			Thread jcThread = new Thread(jc);
			jcThread.start();
			while (true) {
				if (jc.allFinished()) {
					System.out.println(jc.getSuccessfulJobList());
					jc.stop();
					break;
				}
				if (jc.getFailedJobList().size() > 0) {
					System.out.println(jc.getFailedJobList());
					jc.stop();
					break;
				}
			}
		}
}
