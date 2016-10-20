package com.bigdata.bdp.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import com.bigdata.bdp.utils.DateComparator;
import com.bigdata.bdp.utils.Product;
import com.bigdata.bdp.utils.BigDataUtils;

public class ProductPriceTop10VerReducer extends Reducer<Text, Text, NullWritable, Text> {
	private static Logger logger = Logger.getLogger(ProductPriceTop10VerReducer.class);
	
	@Override
	public void setup(Context context) {
		logger.info("Intializing ProductPriceTop10VerReducer setup");
		Configuration conf = context.getConfiguration();
	}
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) {
		logger.info("entering reduce block!!");
		
		try {
			HashMap<String, Product> duplicateRecords = new HashMap<String, Product>();
			
			List<Product> productList = new ArrayList<Product>();
			
			for(Text value : values) {
				String val = value.toString().trim();
				
				String[] strArrVal = val.split(",");
				
				Product product = new Product();
				
				String id = (strArrVal[0] != null && !strArrVal[0].isEmpty()) ? strArrVal[0].trim() : null;
				String product_name = (strArrVal[1] != null && !strArrVal[1].isEmpty()) ? strArrVal[1].trim() : null;
				String price = (strArrVal[2] != null && !strArrVal[2].isEmpty()) ? strArrVal[2].trim() : "NA";
				String date = (strArrVal[3] != null && !strArrVal[3].isEmpty()) ? strArrVal[3].trim() : "NA";
				
				if(id != null) {
					product.setId(id);
					product.setName(product_name);
					product.setPrice(price);
					product.setUpdate_dt(date);
				
				
				String dupKey = new StringBuffer(id).append(",").append(product_name).append(",").append(price).append(",").append(date).toString().trim();
				String dupValue = new StringBuffer(date).toString().trim();
				
				if(duplicateRecords.get(dupKey)==null) {
					duplicateRecords.put(dupKey, product);
					
				} else {
					if(BigDataUtils.convertStringToDate(dupValue).after(BigDataUtils.convertStringToDate(duplicateRecords.get(dupKey).getUpdate_dt()))) {
						duplicateRecords.put(dupKey, product);
						
					}
				}
				
			  }
			}
			
			for(String mapKey : duplicateRecords.keySet()) {
				String mapKey1 = mapKey.toString();
				String price1 = (String)duplicateRecords.get(mapKey1).getPrice().toString();
				String date1 = (String)duplicateRecords.get(mapKey1).getUpdate_dt().toString();
				
				logger.info("duplicateRecordsMap: " + mapKey1 + " : " + price1 +","+ date1);
			}
			
			//productList = new ArrayList<Product>(duplicateRecords.values());
			
			productList.addAll(duplicateRecords.values());
			
			//printing unsorted list
			int i=0;
			for(Product product : productList) {
				logger.info("UnSortedproductlist[" + i + "] :" + product.getId() +","+ product.getName() + "," + product.getPrice() + "," + product.getUpdate_dt());
				i++;
			}
			
			// sorting the list based on captured date in descending order
			Collections.sort(productList, new DateComparator());
			
			//printing sorted list
			int j=0;
			for(Product product : productList) {
				logger.info("Sortedproductlist[" + j + "] :" + product.getId() +","+ product.getName() + "," + product.getPrice() + "," + product.getUpdate_dt());
				j++;
			}
			
			int counter=0;
			
			for(Product product : productList) {
				if(counter == 10) {
					break;
				}
				
				StringBuffer sb = new StringBuffer();
				
				sb.append(product.getId()).append(",").append(product.getName()).append(",")
				  .append(product.getPrice()).append(",").append(product.getUpdate_dt());
				
				context.write(NullWritable.get(), new Text(sb.toString()));
				
				counter++;
			}
		
			/*for (Text value : values) {
				context.write(NullWritable.get(), new Text(value.toString()));
			}*/
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		
	}
}
