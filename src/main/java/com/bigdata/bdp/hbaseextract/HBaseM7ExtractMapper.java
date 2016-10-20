package com.bigdata.bdp.hbaseextract;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bigdata.bdp.constants.Constants;

public class HBaseM7ExtractMapper extends TableMapper<NullWritable, Text> implements Constants{
	private static final Logger logger = LoggerFactory.getLogger(HBaseM7ExtractDriver.class);
	private byte[] COL_FAMILY;
	private String columnFamily;
	private String columnNames;
	
	@Override
	protected void setup(Mapper<ImmutableBytesWritable,Result,NullWritable,Text>.Context context)
			throws IOException, InterruptedException {
		logger.info("Inside setup() of mapper");
		columnFamily = context.getConfiguration().get(HBASE_TABLE_COLUMN_FAMILY);
		COL_FAMILY = columnFamily.getBytes();
		columnNames = context.getConfiguration().get(HBASE_COLUMN_NAME);
	}
	
	@Override
	protected void map(ImmutableBytesWritable row, 
			Result value, Mapper<ImmutableBytesWritable, Result, NullWritable, Text>.Context context)
					throws IOException, InterruptedException {
		logger.info("Inside setup() of mapper");
		
		String[] columns = columnNames.split(COMMA, 2);
		
		String val1 = Bytes.toString(value.getValue(
				COL_FAMILY, Bytes.toBytes(columns[0])));
		String val2 = Bytes.toString(value.getValue(
				COL_FAMILY, Bytes.toBytes(columns[1])));
		
		StringBuffer bf = new StringBuffer(val1).append(",")
				.append(val2);
		
		context.write(NullWritable.get(), new Text(bf.toString()));
		
	}

	@Override
	protected void cleanup(
			Mapper<ImmutableBytesWritable, Result, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.cleanup(context);
	}
	
	
}
