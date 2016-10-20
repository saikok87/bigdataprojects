package com.bigdata.bdp.utils;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import com.bigdata.bdp.constants.Constants;

public class HBaseUtility implements Constants {
	private static final Logger logger = Logger.getLogger(HBaseUtility.class);
	
	public String[] getHbaseColumnNames(Mapper<LongWritable, Text, NullWritable, Text>.Context context){
		return context.getConfiguration().get(HBASE_COLUMN_NAME).split(COMMA);
	}
	
    public void createTable(Configuration conf, String hbaseTable, String columnFamily){
    	try {
			HBaseAdmin admin = new HBaseAdmin(conf);
			HTableDescriptor table = new HTableDescriptor(TableName.valueOf(hbaseTable));
			HColumnDescriptor family = new HColumnDescriptor(columnFamily.getBytes());
			
			table.addFamily(family);
			logger.info("Table " + hbaseTable + " created");
			admin.createTable(table);
			admin.close();
		} catch (MasterNotRunningException e) {
			logger.info("Master not running " + e);
		} catch (ZooKeeperConnectionException e) {
			logger.info("Zookeeper connection issue " + e);
		} catch (IOException e) {
			logger.error("IO Exception" + e);
		}
    }
    
    public void deleteTable(Configuration conf, String hbaseTable){
    	try {
			HBaseAdmin admin = new HBaseAdmin(conf);
			admin.disableTable(hbaseTable);
			admin.deleteTable(hbaseTable);
			admin.close();
			logger.info("Table " + hbaseTable + " deleted");
		} catch (MasterNotRunningException e) {
			logger.info("Master not running " + e);
		} catch (ZooKeeperConnectionException e) {
			logger.info("Zookeeper connection issue " + e);
		} catch (IOException e) {
			logger.error("IO Exception" + e);
		}
    }
    
    public boolean checkIfTableExsits(Configuration conf, String hbaseTable){
    	boolean exists = false;
    	try {
			HBaseAdmin admin = new HBaseAdmin(conf);
			exists = admin.tableExists(hbaseTable);
			admin.close();
		} catch (MasterNotRunningException e) {
			logger.error("Master not running " + e);
		} catch (ZooKeeperConnectionException e) {
			logger.info("Zookeeper connection issue " + e);
		} catch (IOException e) {
			logger.error("IO Exception" + e);
		}
    	if(exists)
    		return true;
    	else
    		return false;
    	
    }
    
    public void disableTable(Configuration conf, String hbaseTable){
    	try {
			HBaseAdmin admin = new HBaseAdmin(conf);
			admin.disableTable(hbaseTable);
			admin.close();
    	} catch (MasterNotRunningException e) {
			logger.error("Master not running " + e);
		} catch (ZooKeeperConnectionException e) {
			logger.info("Zookeeper connection issue " + e);
		} catch (IOException e) {
			logger.error("IO Exception" + e);
		}
    	
    }
    
    public void enableTable(Configuration conf, String hbaseTable){
    	try {
			HBaseAdmin admin = new HBaseAdmin(conf);
			admin.enableTable(hbaseTable);
			admin.close();
    	} catch (MasterNotRunningException e) {
			logger.error("Master not running " + e);
		} catch (ZooKeeperConnectionException e) {
			logger.info("Zookeeper connection issue " + e);
		} catch (IOException e) {
			logger.error("IO Exception" + e);
		}
    	
    }
    
    public boolean checkTableIsEnabled(Configuration conf, String hbaseTable){
		boolean isEnabled = false;
		
    	try {
			HBaseAdmin admin = new HBaseAdmin(conf);
			if(admin.isTableEnabled(hbaseTable))
				isEnabled = true;
			else
				isEnabled = false;
			admin.close();
    	} catch (MasterNotRunningException e) {
			logger.error("Master not running " + e);
		} catch (ZooKeeperConnectionException e) {
			logger.info("Zookeeper connection issue " + e);
		} catch (IOException e) {
			logger.error("IO Exception" + e);
		}
    	
    	return isEnabled;
    	
    	
    }
    

}
