package com.cpl.kpi.ips;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
/**
 * Created by Winstercheng on 2016/11/03
 */

public class Reducer extends org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, IntWritable>{
	Set <Text> set=new HashSet<Text>();
	IntWritable count=new IntWritable();
	@Override
	protected void reduce(Text key,Iterable<Text> values,Context context)throws IOException, InterruptedException {
		for(Text val:values){
			set.add(val);
		}
		count.set(set.size());
		context.write(key,count);
	}
}
