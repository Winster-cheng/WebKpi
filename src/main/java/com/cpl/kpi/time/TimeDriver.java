package com.cpl.kpi.time;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.cpl.kpi.TextTuple;


public class TimeDriver extends Configured implements Tool{
	public int run(String[] arg0) throws Exception {
		Configuration conf=getConf();
		String input="/user/data/kpi/access.20120104.log";
		String output="/user/data/kpi/time/out";
		FileSystem fs=FileSystem.get(conf);
		if(fs.exists(new Path(output))){
			fs.delete(new Path(output), true);
		}
		Job job=Job.getInstance(conf,"time");
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
		
		
		return job.waitForCompletion(true)?0:1;
	}
		public static void main(String[] args) throws Exception {
			int status=ToolRunner.run(new TimeDriver(), args);
			System.exit(status);
		}
}	
