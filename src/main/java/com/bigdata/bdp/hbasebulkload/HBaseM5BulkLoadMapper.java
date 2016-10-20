package com.bigdata.bdp.hbasebulkload;

import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.bigdata.bdp.constants.Constants;

public class HBaseM5BulkLoadMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> implements Constants{
	private static final Logger logger = Logger.getLogger(HBaseM5BulkLoadMapper.class);
	ImmutableBytesWritable hKey = new ImmutableBytesWritable();
	KeyValue kv;
	int rowKeyIndex;
	String[] strArrCols;
	static byte[] COL_FAMILY;
	
	protected void setup(
			Mapper<LongWritable, Text, ImmutableBytesWritable, Put>.Context context) throws IOException,
				InterruptedException {
		logger.info("Inside setup() of mapper");
		String columnFamily = context.getConfiguration().get("hbase.table.column.family");
		COL_FAMILY = columnFamily.getBytes();
		rowKeyIndex = Integer.parseInt(context.getConfiguration().get(HBASE_ROWKEY_INDEX));
		strArrCols = getHbaseColumns(context);
		
	}

	
	private String[] getHbaseColumns(
			org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, ImmutableBytesWritable, Put>.Context context) {
		return context.getConfiguration().get(HBASE_COLUMN_NAME).split(COMMA);
	}
	
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, ImmutableBytesWritable, Put>.Context context) throws IOException, InterruptedException {
		logger.info("Inside map() of mapper");
		
		String fields[] = value.toString().split(COMMA, 2);
		
		if(strArrCols.length == fields.length){
			hKey.set((fields[rowKeyIndex]).getBytes());
			
			if(fields[0] != null && !fields[0].isEmpty() && !fields[0].trim().equalsIgnoreCase("") && !("\\N").equalsIgnoreCase(fields[0])) {
				
				Put put = new Put(Bytes.toBytes(fields[0]));
				put.add(COL_FAMILY, strArrCols[1].getBytes(), fields[1].trim().getBytes());
				context.write(hKey, put);
			}
			else{
				logger.error("Rowkey is empty");
			}
		}
		else{
			logger.error("Mismatch in properties file and input file records");
		}
	}
}
