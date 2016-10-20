package com.bigdata.bdp.hbase;

import java.io.IOException;
import java.io.InterruptedIOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.bigdata.bdp.constants.Constants;

public class UpdateHbaseRow extends Configured implements Constants {
	
	private static Logger logger = Logger.getLogger(UpdateHbaseRow.class);
	private static Configuration conf = null;
	
	public static void main(String[] args) {
		logger.info("Initiating Main method of HBase Update class for update row");
		
		conf = HBaseConfiguration.create();
		
		conf.set(MAPRED_JOB_QUEUE_NAME, "bigdataqueue");
		//conf.set(HBASE_TABLE_NAME, "/path/to/hbase/table/empTest");
		conf.set(HBASE_TABLE_COLUMN_FAMILY,"cf1");
		conf.set(HBASE_COLUMN_NAME, "col1");
		
		try {
			HTable hTable = new HTable(conf, "/path/to/hbase/table/empRaw");
			
			Put put = new Put(Bytes.toBytes("row1"));
			
			put.add(Bytes.toBytes("cf1"), Bytes.toBytes("col1"), Bytes.toBytes("Mumbai"));
			
			hTable.put(put);
			
			logger.info("data updated");
			
			hTable.close();
		} catch (RetriesExhaustedWithDetailsException e) {
			e.printStackTrace();
		} catch (InterruptedIOException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

}