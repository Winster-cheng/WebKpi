package com.cpl.kpi;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
/**
 * Created by winstercheng on 16/11/09.
 * 实现最终结果从大到小排序
 * 
 */
public class TextTuple implements WritableComparable {
	public String k;
	public IntWritable v;
	public TextTuple(){
		super();
	}
	public TextTuple(String k,Integer v){
		this.k=k;
		this.v.set(v);
	}
	public String getK() {
		return k;
	}
	public void setK(String k) {
		this.k = k;
	}
	public IntWritable getV() {
		return v;
	}
	public void setV(IntWritable v) {
		this.v = v;
	}

	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}
	public int compareTo(Object o) {
		TextTuple b=(TextTuple)o;
		if(this.v.compareTo(b.getV())>0){
			return 1;
		}else if(this.v.compareTo(b.getV())<0)
			return -1;
		else
		return this.k.compareTo(b.getK());
	}

}
