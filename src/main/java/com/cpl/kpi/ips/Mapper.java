package com.cpl.kpi.ips;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import com.cpl.kpi.Kpi;

/**
 * Created by Winstercheng on 2016/11/03
 */
public class Mapper
		extends
		org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text> {

	Kpi kpi = new Kpi();
	Text remote_addr=new Text();
	Text request_page=new Text();
	@Override
	protected void map(LongWritable key, Text value,org.apache.hadoop.mapreduce.Mapper.Context context)throws IOException, InterruptedException {
			kpi=Kpi.parse(value.toString());
			if(kpi.getIs_validate()){
				remote_addr.set(kpi.getRemote_addr());  //访问的ip
				request_page.set(kpi.getRequest_page());//请求的网页;
				context.write(request_page, remote_addr);
			}
	}
}