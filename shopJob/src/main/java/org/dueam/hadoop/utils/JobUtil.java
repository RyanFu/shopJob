package org.dueam.hadoop.utils;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class JobUtil {
	/**
	 * 判断一个字符串是否正整数
	 * 
	 * public static boolean isPositiveInteger(String s) { if
	 * (StringUtils.isNotBlank(s) && StringUtils.isNumeric(s) && Long.valueOf(s)
	 * > 0) { return true; } return false; }
	 */

	/**
	 * 根据日期生成WeekId 日期格式:20101031 weekId格式:年+周id eg:201001
	 */
	public static int createWeekIdByDateStr(String dateStr) {

		Calendar cal = Calendar.getInstance(Locale.CHINA);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");

		try {
			cal.setTime(sdf.parse(dateStr));
		} catch (Exception e) {
			return 0;
		}

		cal.setFirstDayOfWeek(2);// 设定星期一是一周的第一天
		cal.setMinimalDaysInFirstWeek(7);// 设定必须满7天才能算一周

		int dayOfWeek = cal.get(Calendar.DAY_OF_WEEK);

		if (dayOfWeek == 1) {// 如果是星期天
			dayOfWeek = dayOfWeek + 7;
		}

		cal.add(Calendar.DAY_OF_YEAR, 2 - dayOfWeek);// 减去距离本周一的天数
		return cal.get(Calendar.YEAR) * 100 + cal.get(Calendar.WEEK_OF_YEAR);

	}

	/**
	 * 生成8天前的时间戳  eg:20101031
	 */
	public static String createLastWeekDateStr() {
		Calendar cal = Calendar.getInstance(Locale.CHINA);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		cal.setTime(new Date());
		cal.add(Calendar.DAY_OF_YEAR, -8);// 前天
		return sdf.format(cal.getTime());
	}
	
	/**
	 * 生成前今天的数据(但是时间戳是前一天的)  eg:20101031
	 */
	public static String createDateStr() {
		Calendar cal = Calendar.getInstance(Locale.CHINA);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		cal.setTime(new Date());
		cal.add(Calendar.DAY_OF_YEAR, -1);// 前天
		return sdf.format(cal.getTime());
	}
	
	/**
	 * 根据现在时间生成前天的日期 eg:20101031
	 */
	public static String createDateBefYesterday() {
		Calendar cal = Calendar.getInstance(Locale.CHINA);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		cal.setTime(new Date());
		cal.add(Calendar.DAY_OF_YEAR, -2);// 前天
		return sdf.format(cal.getTime());
	}
	
	/**
	 * 判断HDFS是否存在该目录,如果存在则DEL掉
	 * @param args
	 */
	public static void delDir(String path) throws IOException{
		
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(configuration);
	    Path inputTotal = new Path(path);

	    if (fs.exists(inputTotal)) {
	    	fs.delete(inputTotal, true);
	    }
	    
	    System.out.println("----->del:" + path);
	  
	
	}
	
	/**
	 * 判断HDFS是否存在该目录,如果存在则DEL掉
	 * @param args
	 */
	public static void delDir(String path,Configuration configuration) throws IOException{
		
		FileSystem fs = FileSystem.get(configuration);
	    Path inputTotal = new Path(path);

	    if (fs.exists(inputTotal)) {
	    	fs.delete(inputTotal, true);
	    }	  
	
	}
	
	/**
	 * 判断HDFS是否存在该目录
	 * @param args
	 */
	public static boolean isExist(String path) throws IOException{
		
		boolean isExist=false;
		Configuration configuration = new Configuration();
		
		FileSystem fs = FileSystem.get(configuration);
	    Path inputTotal = new Path(path);

	    if (fs.exists(inputTotal)) {
	    	isExist=true;
	    }
	  
	    return isExist;
	
	}
	
	/**
	 * 判断HDFS是否存在该目录
	 * @param args
	 */
	public static boolean isExist(String path,Configuration configuration) throws IOException{
		
		boolean isExist=false;
		
		FileSystem fs = FileSystem.get(configuration);
	    Path inputTotal = new Path(path);

	    if (fs.exists(inputTotal)) {
	    	isExist=true;
	    }
	  
	    return isExist;
	
	}
	
	/**
	 * 根据当前日期算上周某一天的日期,假如dayNum=0 获得上周一的日期 dayNum=1 获得上周二的日期 dayNum=6 获得上周日的日期
	 */
	public static String convertWeekByDate(String time, int dayNum) {   
		  
        SimpleDateFormat sdf=new SimpleDateFormat("yyyyMMdd"); //设置时间格式   
        Calendar cal = Calendar.getInstance(); 
        try{
        cal.setTime(sdf.parse(time));   
        }catch(Exception e){
        	return null;
        }
        //判断要计算的日期是否是周日，如果是则减一天计算周六的，否则会出问题，计算到下一周去了   
        int dayWeek = cal.get(Calendar.DAY_OF_WEEK);//获得当前日期是一个星期的第几天   
        if(1 == dayWeek) {   
            cal.add(Calendar.DAY_OF_MONTH, -1);   
        }   
        //System.out.println("要计算日期为:"+sdf.format(cal.getTime())); //输出要计算日期   
        cal.setFirstDayOfWeek(Calendar.MONDAY);//设置一个星期的第一天，按中国的习惯一个星期的第一天是星期一   
        int day = cal.get(Calendar.DAY_OF_WEEK);//获得当前日期是一个星期的第几天   
        cal.add(Calendar.DATE, cal.getFirstDayOfWeek()-day-7+dayNum);//根据日历的规则，给当前日期减去星期几与一个星期第一天的差值    
        String imptimeBegin = sdf.format(cal.getTime());   
        return imptimeBegin;
           
    }  
	
	
	/**
	 * 获取当前时间
	 * @param args
	 */
	
	public static String getNowDate(){
		SimpleDateFormat sdf=new SimpleDateFormat("yyyyMMdd"); //设置时间格式   
		Calendar cal = Calendar.getInstance(); 
		String nowDate=sdf.format(cal.getTime());
		
		return nowDate;
	}
	

	/**
	 * 判断一个字符串是否是数字，包括小数
	 * @param args
	 */
	public static boolean isNum(String str){
		try{
			Double.parseDouble(str);
			return true;
		}catch(Exception e){
			return false;
		}
	}
	
	/**
	 * 获得周几  当i=0 获得当天是周几 i=-1 获得昨天是周几 当i=-2获得前台是周几 
	 */
	public static int getDay(int i){	
		Calendar cal = Calendar.getInstance(); 
		cal.add(Calendar.DAY_OF_YEAR, i-1);
		int day = cal.get(Calendar.DAY_OF_WEEK);		
		return day;
	}


	public static void main(String[] args) {
		
		System.out.println(createWeekIdByDateStr("20100107"));
		System.out.println(convertWeekByDate("20100101",6));
		System.out.println(getNowDate());
		System.out.println(getDay(-2));
		System.out.println(createDateBefYesterday());


	}
}
