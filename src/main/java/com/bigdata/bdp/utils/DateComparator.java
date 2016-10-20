package com.bigdata.bdp.utils;

import java.util.Comparator;
import java.util.Date;

public class DateComparator implements Comparator<Product>{

	@Override
	public int compare(Product o1, Product o2) {
		
		String date1 = o1.getUpdate_dt();
		String date2 = o2.getUpdate_dt();
		int result=0;
		
		if(date1 != null && date2 != null && date1.length() > 0 && date2.length() > 0) {
			Date d1 = BigDataUtils.convertStringToDate(date1);
			Date d2 = BigDataUtils.convertStringToDate(date2);
			
			if(d1.after(d2)) {
				result = -1;
			} else if (d1.before(d2)) {
				result = 2;
			} else {
				result = 0;
			}
		}
		
		
		return result;
	}
	

}
