package com.bigdata.bdp.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class AverageSalaryMapper extends Mapper<LongWritable, Text, Text, FloatWritable>{
	
	private static final Logger logger = Logger.getLogger(AverageSalaryMapper.class);
	
	@Override
	public void setup(Mapper<LongWritable,Text,Text,FloatWritable>.Context context) {
		logger.info("Intializing IncomeBnkAffiliateCalMapper setup");
		Configuration conf = context.getConfiguration();
	}
	
	@Override
	public void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FloatWritable>.Context context) {
		logger.info("AverageSalaryMapper Starts");
		
		String line = value.toString();
		String[] values = line.split(",");
		String dept = values[3];
		
		try {
			Float salary = Float.parseFloat(values[2]);
			context.write(new Text(dept), new FloatWritable(salary));
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		
		
	}
	
}
