package com.cpl.kpi.pv;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class Reducer extends org.apache.hadoop.mapreduce.Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context)throws IOException, InterruptedException {
			int count=0;
			for(IntWritable i:values){
				count++;
			}
		context.write(key, new IntWritable(count));
		}
}
