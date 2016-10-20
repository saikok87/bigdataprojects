package com.bigdata.bdp.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class ProductPriceTop10VerMapper extends Mapper<LongWritable, Text, Text, Text> {
	private static Logger logger = Logger.getLogger(ProductPriceTop10VerMapper.class);
	
	@Override
	public void setup(Mapper<LongWritable, Text, Text, Text>.Context context) {
		logger.info("Intializing ProductPriceTop10VerMapper..");
		Configuration conf = context.getConfiguration();
	}
	
	@Override
	public void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] values = line.split(",");
		String id = values[0];
		String product_name = values[1];
		String price = values[2];
		String date = values[3];
		
		//key
		StringBuffer sbKey = new StringBuffer();
		sbKey.append(id).append("_").append(product_name);
		
		//value
		StringBuffer sb = new StringBuffer();
		sb.append(id).append(",").append(product_name).append(",").append(price).append(",").append(date);
		
		
		logger.info("Mapper Key: " + sbKey.toString());
		logger.info("Mapper values: " + sb.toString());
		
		context.write(new Text(sbKey.toString()), new Text(sb.toString()));
		
	}
}
