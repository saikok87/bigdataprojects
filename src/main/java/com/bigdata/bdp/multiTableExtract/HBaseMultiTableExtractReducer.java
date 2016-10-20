package com.bigdata.bdp.multiTableExtract;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bigdata.bdp.constants.Constants;
import com.bigdata.bdp.exception.BDPException;

public class HBaseMultiTableExtractReducer extends TableReducer<Text, IntWritable, ImmutableBytesWritable> implements Constants{
	
	private static final Logger logger = LoggerFactory.getLogger(HBaseMultiTableExtractMapper.class);
	private byte[] TOTAL_SALES_COL_FAMILY;
	private byte[] TOTALE_SALES_COL;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException{
		TOTAL_SALES_COL_FAMILY = context.getConfiguration().get(TOTAL_SALES_COLUMN_FAMILY).getBytes();
		TOTALE_SALES_COL = context.getConfiguration().get(TOTAL_SALES_COLUMN_NAME).getBytes();
	}
	
	public void reduce(Text key, Iterable<IntWritable> values, Context context){
		if(key.toString().startsWith("s")){
			Integer dayStoreSales = 0;
			for(IntWritable storeSales: values){
				dayStoreSales = dayStoreSales + new Integer(storeSales.toString());
			}
			
			Put put = new Put(Bytes.toBytes(key.toString()));
			put.add(TOTAL_SALES_COL_FAMILY, TOTALE_SALES_COL, Bytes.toBytes(dayStoreSales));
			
			try {
				context.write(null, put);
			} catch (IOException e) {
				logger.error("HBase MultiTable extarct Job failed in reduce() IOException:" + e);
				try {
					logger.error("HBase MultiTable extarct Job failed in reduce() IOException:",e);
					throw new BDPException("HBase MultiTable extarct Job failed in reduce():",e);
				} catch (BDPException e1) {
					logger.error("HBase MultiTable extarct Job failed in reduce() BDPException:",e1);
				}
			} catch (InterruptedException e) {
				logger.error("HBase MultiTable extarct Job failed in reduce() InterruptedException:",e);
				try {
					logger.error("HBase MultiTable extarct Job failed in reduce() InterruptedException:",e);
					throw new BDPException("HBase MultiTable extarct Job failed in reduce():",e);
				} catch (BDPException e1) {
					logger.error("HBase MultiTable extarct Job failed in reduce() BDPException:",e1);
				}
			}
			
		} else {
			Integer dayStoreSales = 0;
			for(IntWritable storeSales: values){
				dayStoreSales = dayStoreSales + new Integer(storeSales.toString());
			}
			
			Put put = new Put(Bytes.toBytes(key.toString()));
			put.add(TOTAL_SALES_COL_FAMILY, TOTALE_SALES_COL, Bytes.toBytes(dayStoreSales));
			
			try {
				context.write(null, put);
			} catch (IOException e) {
				logger.error("HBase MultiTable extarct Job failed in reduce() IOException:" + e);
				try {
					logger.error("HBase MultiTable extarct Job failed in reduce() IOException:",e);
					throw new BDPException("HBase MultiTable extarct Job failed in reduce():",e);
				} catch (BDPException e1) {
					logger.error("HBase MultiTable extarct Job failed in reduce() BDPException:",e1);
				}
			} catch (InterruptedException e) {
				logger.error("HBase MultiTable extarct Job failed in reduce() InterruptedException:",e);
				try {
					logger.error("HBase MultiTable extarct Job failed in reduce() InterruptedException:",e);
					throw new BDPException("HBase MultiTable extarct Job failed in reduce():",e);
				} catch (BDPException e1) {
					logger.error("HBase MultiTable extarct Job failed in reduce() BDPException:",e1);
				}
			}
			
		}
	}
}
