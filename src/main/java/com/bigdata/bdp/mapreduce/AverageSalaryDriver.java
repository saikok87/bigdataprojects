package com.bigdata.bdp.mapreduce;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.bigdata.bdp.constants.Constants;
import com.bigdata.bdp.utils.BigDataUtils;

public class AverageSalaryDriver extends Configured implements Tool, Constants {
	
	private Configuration conf;
	private static FileSystem fs;
	private static Logger logger = Logger.getLogger(AverageSalaryDriver.class);
	
	public static void main(String[] args) throws Exception {
		AverageSalaryDriver avgAverageSalaryDriver = new AverageSalaryDriver();
		long start = System.currentTimeMillis();
		int exit = ToolRunner.run(avgAverageSalaryDriver, args);
		long end = System.currentTimeMillis();
		logger.info("Total time taken by the utility: " + (end-start) + " millisecs");
		
		if(exit!=0) {
			logger.info("AverageSalary calculator failed with exit code " + exit);
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		String inputDataset = args[0];
		String outputDataset = args[1];
		String configFile = args[2];
		
		//set configuration properties
		conf = getConf();
		
		//get hold of hadoop file system
		fs = FileSystem.get(conf);
		
		//validate input dataset
		Path path = new Path(inputDataset);
		if(!fs.exists(path)) {
			logger.warn("Input directory " + path + " doesnot exists");
			return -1;
		}
		
		//load properties file
		Properties props = BigDataUtils.loadProperties(configFile, fs);
		
		conf.set(MAPRED_JOB_QUEUE_NAME, props.getProperty(MAPRED_JOB_QUEUE_NAME));
		conf.set(OUTPUT_PATH, outputDataset);
		
		Job job = new Job(conf, "MR: Calculating Total and Avg salary");
		job.setJarByClass(AverageSalaryDriver.class);
		job.setMapperClass(AverageSalaryMapper.class);
		job.setReducerClass(AverageSalaryReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FloatWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        logger.info("Input Path to the map-reduce job - " + inputDataset);
        FileInputFormat.addInputPath(job, path);
        
        logger.info("Ouput Path to the map-reduce job - " + outputDataset);
        Path output = new Path(outputDataset);
        FileOutputFormat.setOutputPath(job, output);
        
        // delete output if existing
        logger.info("Deleting output path " + outputDataset);
        FileSystem.get(conf).delete(new Path(outputDataset),true);
        
        int status = job.waitForCompletion(true) ? 0 : 1;
        
        return status;
	}

}
