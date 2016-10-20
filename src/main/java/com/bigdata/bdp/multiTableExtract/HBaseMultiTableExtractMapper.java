package com.bigdata.bdp.multiTableExtract;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bigdata.bdp.constants.Constants;
import com.bigdata.bdp.exception.BDPException;

public class HBaseMultiTableExtractMapper extends TableMapper<Text, IntWritable> implements Constants{
	private static final Logger logger = LoggerFactory.getLogger(HBaseMultiTableExtractMapper.class);
	private byte[] storeSalesTableName,onlineSalesTableName;
	private byte[] STORE_SALES_COLUMN_FAMILY,ONLINE_SALES_COLUMN_FAMILY;
	private byte[] ONLINE_SALES_COLUMN,STORE_SALES_COLUMN;
	private String storeSales,onlineSales;
	private Integer sSales,oSales;
	private byte[] sales;
	private Text mapperKey;
	private IntWritable mapperValue;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException{
		storeSalesTableName = context.getConfiguration().get(STORE_SALES_TABLE_NAME).getBytes();
		onlineSalesTableName = context.getConfiguration().get(ONLINE_SALES_TABLE_NAME).getBytes();
		
		STORE_SALES_COLUMN_FAMILY = context.getConfiguration().get(STORE_SALES_COLUMN_FAMILY_NAME).getBytes();
		ONLINE_SALES_COLUMN_FAMILY = context.getConfiguration().get(ONLINE_SALES_COLUMN_FAMILY_NAME).getBytes();
		
		STORE_SALES_COLUMN = context.getConfiguration().get(STORE_SALES_COLUMN_NAME).getBytes();
		ONLINE_SALES_COLUMN = context.getConfiguration().get(ONLINE_SALES_COLUMN_NAME).getBytes();
		
	}
	
	protected void map(ImmutableBytesWritable rowKey, Result columns, Context context) 
			throws IOException, InterruptedException{
		
		//get table name
		TableSplit currentSplit = (TableSplit)context.getInputSplit();
		byte[] tableName = currentSplit.getTableName();
		
		try {
			if(Arrays.equals(tableName,storeSalesTableName)){
				String date = new String(rowKey.get()).split("#")[0];
				sales = columns.getValue(STORE_SALES_COLUMN_FAMILY, STORE_SALES_COLUMN);
				storeSales = new String(sales);
				sSales = new Integer(storeSales);
				mapperKey = new Text("s#" + date);
				mapperValue =  new IntWritable(sSales);
				context.write(mapperKey, mapperValue);
			} else if (Arrays.equals(tableName,onlineSalesTableName)) {
				String date = new String(rowKey.get());
				sales = columns.getValue(ONLINE_SALES_COLUMN_FAMILY, ONLINE_SALES_COLUMN);
				onlineSales = new String(sales);
				oSales = new Integer(onlineSales);
				mapperKey = new Text("o#" + date);
				mapperValue =  new IntWritable(oSales);
				context.write(mapperKey, mapperValue);
			}
		} catch (Exception e) {
			logger.error("HBase MultiTable extarct Job failed in map():" + e);
			try {
				logger.error("HBase MultiTable extarct Job failed in map():" + e);
				throw new BDPException("HBase MultiTable extarct Job failed in map(): " + e);
			} catch (BDPException e1) {
				logger.error("Exception occured while throwing BDPException:" + e1.getMessage());
			}
		}
		
		
	}
	

}
