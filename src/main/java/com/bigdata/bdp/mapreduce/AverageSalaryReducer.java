package com.bigdata.bdp.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class AverageSalaryReducer extends Reducer<Text, FloatWritable, Text, Text>{
	
	private static Logger logger = Logger.getLogger(AverageSalaryReducer.class);
	
	@Override
	public void setup(Context context) {
		logger.info("Intializing AverageSalaryReducer setup");
		Configuration conf = context.getConfiguration();
	}
	
	@Override
	public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
		
		Float total = (float) 0;
		int count = 0;
		
		for(FloatWritable val : values) {
			total= total + val.get();
			logger.info("salary: " + val.get());
			count++;
		}
		
		Float avg = (Float) total / count;
		String out = "Total: " + total + " Average: " + avg;
		
		context.write(key, new Text(out));
		
	}
}
