package com.cpl.kpi.source;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import com.cpl.kpi.Kpi;

public class Mapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text,Text,IntWritable >{
	Text source = new Text();
    IntWritable one = new IntWritable(1);
    Kpi kpi = new Kpi();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        kpi = Kpi.parse(value.toString());
        if (kpi.getIs_validate()) {
            source.set(kpi.getHttp_referrer());
            context.write(source, one);
        }
    }
}
