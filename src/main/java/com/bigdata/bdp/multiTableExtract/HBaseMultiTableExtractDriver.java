package com.bigdata.bdp.multiTableExtract;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bigdata.bdp.constants.Constants;
import com.bigdata.bdp.exception.BDPException;
import com.bigdata.bdp.utils.HBaseUtility;
import com.bigdata.bdp.utils.BigDataUtils;

public class HBaseMultiTableExtractDriver extends Configured implements Tool, Constants  {

	private static final Logger logger = LoggerFactory.getLogger(HBaseMultiTableExtractDriver.class);
	private Configuration conf;
	private static FileSystem fs;
	private static Properties props;
	
    @Override
	public int run(String[] args) throws Exception {
		String configFile = args[0];
		String storeSalesTable = args[1];
		String onlineSalesTable = args[2];
		String storeSalesColumnFamily = args[3];
		String onlineSalesColumnFamily = args[4];
		String totalSalesColumnFamily = args[5];
		String totalSalesTable = args[6];
		
		List<Scan> scans = new ArrayList<Scan>();
		
		conf = getConf();
		// get hold of hadoop file system
		fs = FileSystem.get(conf);
		
		props = BigDataUtils.loadProperties(configFile, fs);
		
		conf = HBaseConfiguration.create();
		
		conf.set(MAPRED_JOB_QUEUE_NAME, props.getProperty(MAPRED_JOB_QUEUE_NAME));
		conf.set(STORE_SALES_TABLE_NAME, storeSalesTable);
		conf.set(ONLINE_SALES_TABLE_NAME, onlineSalesTable);
		conf.set(ONLINE_SALES_COLUMN_FAMILY_NAME, onlineSalesColumnFamily);
		conf.set(STORE_SALES_COLUMN_FAMILY_NAME, storeSalesColumnFamily);
		conf.set(ONLINE_SALES_COLUMN_NAME, props.getProperty(ONLINE_SALES_COLUMN_NAME));
		conf.set(STORE_SALES_COLUMN_NAME, props.getProperty(STORE_SALES_COLUMN_NAME));
		conf.set(TOTAL_SALES_COLUMN_FAMILY, totalSalesColumnFamily);
		conf.set(TOTAL_SALES_COLUMN_NAME, props.getProperty(TOTAL_SALES_COLUMN_NAME));
		
		
		HBaseUtility utility = new HBaseUtility();
		
		if(!utility.checkIfTableExsits(conf, storeSalesTable)){
			logger.warn("HBase Table" + storeSalesTable + " does not exist");
			return -1;
		}
		if(!utility.checkIfTableExsits(conf, onlineSalesTable)){
			logger.warn("HBase Table" + onlineSalesTable + " does not exist");
			return -1;
		}
		
		Job job = Job.getInstance(conf, "MapReduce:MultitableInput for HBase M7");
		
		job.setJarByClass(HBaseMultiTableExtractDriver.class);
		
		Scan scanStore = new Scan();
		scanStore.setAttribute("scan.attributes.table.name", Bytes.toBytes(storeSalesTable));
		scanStore.setCaching(500); // 1 is the default in Scan, which will be bad for MapReduce jobs
		scanStore.setCacheBlocks(false); // don't set to true for MR jobs
		logger.info(Bytes.toString(scanStore.getAttribute("scan.attributes.table.name")));
		scans.add(scanStore);
		
		Scan scanOnline = new Scan();
		scanOnline.setAttribute("scan.attributes.table.name", Bytes.toBytes(onlineSalesTable));
		scanOnline.setCaching(500); // 1 is the default in Scan, which will be bad for MapReduce jobs
		scanOnline.setCacheBlocks(false); // don't set to true for MR jobs
		logger.info(Bytes.toString(scanOnline.getAttribute("scan.attributes.table.name")));
		scans.add(scanOnline);
		
		TableMapReduceUtil.initTableMapperJob(scans, HBaseMultiTableExtractMapper.class, Text.class, IntWritable.class, job);
		
		TableMapReduceUtil.initTableReducerJob(totalSalesTable, HBaseMultiTableExtractReducer.class,job);
		
		int status = job.waitForCompletion(true) ? 0 : 1;
		return status;
		
	}

    public static void main(String[] args) throws BDPException {
    	logger.info("Initiating Main method of HBase Driver class for MultiTableExtract");
    	HBaseMultiTableExtractDriver driver = new HBaseMultiTableExtractDriver();
    	driver.initiate(args);
    	
    }

	private void initiate(String[] args) {
		int exit = 0;
		
		try {
			exit = ToolRunner.run(new HBaseMultiTableExtractDriver(), args);
			if(exit != 0){
				logger.error("HBase MultiTable extarct Job failed with error exit code:" + exit);
				throw new BDPException("HBase MultiTable extarct Job failed with error exit code: " + exit);
			}	
		} catch (Exception e) {
			logger.error("HBase MultiTable extarct Job failed with error exit code:" + exit,e);
			try {
				throw new BDPException("HBase MultiTable extarct Job failed with error exit code:" + exit,e);
			} catch (BDPException e1) {
				logger.error("Exception occured while throwing BDPException:" + e1.getMessage());
			}
			
		}
		
		
	}

}
