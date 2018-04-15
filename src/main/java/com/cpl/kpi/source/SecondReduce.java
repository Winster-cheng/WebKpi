package com.cpl.kpi.source;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable.Comparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SecondReduce extends Configured {
	static 	class KpiSort extends Comparator{
		 public int compare( IntWritable a,IntWritable b){  
			 return -super.compare(a,b);
		 }  
	     public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {  
	         return -super.compare(b1, s1, l1, b2, s2, l2);  
	     }  
	}
	static class WordCountMapper 
	extends Mapper<LongWritable, Text, IntWritable, Text>{
		private Text word=new Text();
		private IntWritable count=new IntWritable();
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String s[]=value.toString().split("	");
				word.set(s[0]);
				count.set(Integer.parseInt(s[1]));
				context.write(count,word);
		}
	}

	static class WordCountReducer extends Reducer<IntWritable, Text, Text, IntWritable>{
		@Override
		protected void reduce(IntWritable key, 
				Iterable<Text> values,
				Reducer<IntWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int count=0;
			for(Text one:values){
				context.write(one,key);
			}
		
		}
	}

	public static  Job getJob2() throws Exception {
		//获得程序运行时的配置信息
		Configuration conf=new Configuration();
		String inputPath="/user/data/kpi/source/out/part-r-00000";
		String outputPath="/user/data/kpi/source/out/sort";
		FileSystem fs=FileSystem.get(conf);
		if(fs.exists(new Path(outputPath))){
			fs.delete(new Path(outputPath), true);
		}	
		//构建新的作业
		Job job = Job.getInstance(conf, "source second");
		job.setJarByClass(SecondReduce.class);
		
 		//给job设置mapper类及map方法输出的键值类型
		job.setMapperClass(WordCountMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		//给job设置reducer类及reduce方法输出的键值类型
		job.setReducerClass(WordCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//设置数据的读取方式（文本文件）及结果的输出方式（文本文件）
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		//设置输入和输出目录
		TextInputFormat.addInputPath(job, new Path(inputPath));
		TextOutputFormat.setOutputPath(job, new Path(outputPath));
		 job.setSortComparatorClass(KpiSort.class);
		
		//将作业提交集群执行	
		return job;
	}
	
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException, Exception {
		 System.exit(getJob2().waitForCompletion(true) ? 0 : 1);
	}
}