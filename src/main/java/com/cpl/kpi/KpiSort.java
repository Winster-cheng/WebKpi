package com.cpl.kpi;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.IntWritable.Comparator;


/**
 * Created by winstercheng on 16/11/09.
 * 实现最终结果从大到小排序
 * 
 */
public class KpiSort extends Comparator{
	 public int compare( IntWritable a,IntWritable b){  
		 return -super.compare(a,b);
	 }  
     public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {  
         return -super.compare(b1, s1, l1, b2, s2, l2);  
     }  
}
