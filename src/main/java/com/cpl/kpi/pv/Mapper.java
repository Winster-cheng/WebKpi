package com.cpl.kpi.pv;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import com.cpl.kpi.Kpi;

public class Mapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text,Text,IntWritable >{
	IntWritable v=new IntWritable(1);
	Text k=new Text(); 
	Kpi kpi=null;
	@Override
	protected void map(LongWritable key, Text value,org.apache.hadoop.mapreduce.Mapper.Context context)throws IOException, InterruptedException {
		kpi=Kpi.parse(value.toString());
		if(kpi.getIs_validate()){
		k.set(kpi.getRemote_addr());
		context.write(k,v);
		}
	}
}
