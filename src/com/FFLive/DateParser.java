package com.FFLive;

import java.util.Calendar;
import java.util.HashMap;

public class DateParser {

	public String inDate = new String();
	private	HashMap<String, String[]> month = new HashMap<String, String[]>();
	
	public DateParser() {
		month.put("Aug", new String[] {"8", "2014"});
		month.put("Sep", new String[] {"9", "2014"});
		month.put("Oct", new String[] {"10", "2014"});
		month.put("Nov", new String[] {"11", "2014"});
		month.put("Dec", new String[] {"12", "2014"});
		month.put("Jan", new String[] {"1", "2015"});
		month.put("Feb", new String[] {"2", "2015"});
		month.put("Mar", new String[] {"3", "2015"});
		month.put("Apr", new String[] {"4", "2015"});
		month.put("May", new String[] {"5", "2015"});
		month.put("Jun", new String[] {"6", "2015"});
		month.put("Jul", new String[] {"7", "2015"});
	}
	
	public DateParser(String date) {
		inDate = date;
		month.put("Aug", new String[] {"7", "2014"});
		month.put("Sep", new String[] {"8", "2014"});
		month.put("Oct", new String[] {"9", "2014"});
		month.put("Nov", new String[] {"10", "2014"});
		month.put("Dec", new String[] {"11", "2014"});
		month.put("Jan", new String[] {"0", "2015"});
		month.put("Feb", new String[] {"1", "2015"});
		month.put("Mar", new String[] {"2", "2015"});
		month.put("Apr", new String[] {"3", "2015"});
		month.put("May", new String[] {"4", "2015"});
		month.put("Jun", new String[] {"5", "2015"});
		month.put("Jul", new String[] {"6", "2015"});
	}
	
	public Calendar convertDate() {
		
		//Output 0Year, 1Month, 2Day, 3Hour, 4Min
		int[] converted = new int[5];
		
		//Make ready for conversion
		String[] dateParts = inDate.trim().split(" ");
		String[] time = dateParts[2].split(":");
		//First Part Day, Month, Time HH:MM
		converted[0] = Integer.parseInt(month.get(dateParts[1])[1]);
		converted[1] = Integer.parseInt(month.get(dateParts[1])[0]);
		converted[2] = Integer.parseInt(dateParts[0]);
		converted[3] = Integer.parseInt(time[0].trim());
		converted[4] = Integer.parseInt(time[1].trim());
		

		//Calendar rightNow = Calendar.getInstance();
		Calendar date = Calendar.getInstance();
		date.set(converted[0],converted[1],converted[2],converted[3],converted[4],0);
		
		//date.add(Calendar.HOUR_OF_DAY, 2);
		
		/*System.out.println(rightNow.getTime());
		System.out.println(date.getTime());
		System.out.println(rightNow.before(date));*/
		
		return date;
		
	}
}
