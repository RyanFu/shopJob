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
	 * �ж�һ���ַ����Ƿ�������
	 * 
	 * public static boolean isPositiveInteger(String s) { if
	 * (StringUtils.isNotBlank(s) && StringUtils.isNumeric(s) && Long.valueOf(s)
	 * > 0) { return true; } return false; }
	 */

	/**
	 * ������������WeekId ���ڸ�ʽ:20101031 weekId��ʽ:��+��id eg:201001
	 */
	public static int createWeekIdByDateStr(String dateStr) {

		Calendar cal = Calendar.getInstance(Locale.CHINA);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");

		try {
			cal.setTime(sdf.parse(dateStr));
		} catch (Exception e) {
			return 0;
		}

		cal.setFirstDayOfWeek(2);// �趨����һ��һ�ܵĵ�һ��
		cal.setMinimalDaysInFirstWeek(7);// �趨������7�������һ��

		int dayOfWeek = cal.get(Calendar.DAY_OF_WEEK);

		if (dayOfWeek == 1) {// �����������
			dayOfWeek = dayOfWeek + 7;
		}

		cal.add(Calendar.DAY_OF_YEAR, 2 - dayOfWeek);// ��ȥ���뱾��һ������
		return cal.get(Calendar.YEAR) * 100 + cal.get(Calendar.WEEK_OF_YEAR);

	}

	/**
	 * ����8��ǰ��ʱ���  eg:20101031
	 */
	public static String createLastWeekDateStr() {
		Calendar cal = Calendar.getInstance(Locale.CHINA);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		cal.setTime(new Date());
		cal.add(Calendar.DAY_OF_YEAR, -8);// ǰ��
		return sdf.format(cal.getTime());
	}
	
	/**
	 * ����ǰ���������(����ʱ�����ǰһ���)  eg:20101031
	 */
	public static String createDateStr() {
		Calendar cal = Calendar.getInstance(Locale.CHINA);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		cal.setTime(new Date());
		cal.add(Calendar.DAY_OF_YEAR, -1);// ǰ��
		return sdf.format(cal.getTime());
	}
	
	/**
	 * ��������ʱ������ǰ������� eg:20101031
	 */
	public static String createDateBefYesterday() {
		Calendar cal = Calendar.getInstance(Locale.CHINA);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		cal.setTime(new Date());
		cal.add(Calendar.DAY_OF_YEAR, -2);// ǰ��
		return sdf.format(cal.getTime());
	}
	
	/**
	 * �ж�HDFS�Ƿ���ڸ�Ŀ¼,���������DEL��
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
	 * �ж�HDFS�Ƿ���ڸ�Ŀ¼,���������DEL��
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
	 * �ж�HDFS�Ƿ���ڸ�Ŀ¼
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
	 * �ж�HDFS�Ƿ���ڸ�Ŀ¼
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
	 * ���ݵ�ǰ����������ĳһ�������,����dayNum=0 �������һ������ dayNum=1 ������ܶ������� dayNum=6 ��������յ�����
	 */
	public static String convertWeekByDate(String time, int dayNum) {   
		  
        SimpleDateFormat sdf=new SimpleDateFormat("yyyyMMdd"); //����ʱ���ʽ   
        Calendar cal = Calendar.getInstance(); 
        try{
        cal.setTime(sdf.parse(time));   
        }catch(Exception e){
        	return null;
        }
        //�ж�Ҫ����������Ƿ������գ���������һ����������ģ����������⣬���㵽��һ��ȥ��   
        int dayWeek = cal.get(Calendar.DAY_OF_WEEK);//��õ�ǰ������һ�����ڵĵڼ���   
        if(1 == dayWeek) {   
            cal.add(Calendar.DAY_OF_MONTH, -1);   
        }   
        //System.out.println("Ҫ��������Ϊ:"+sdf.format(cal.getTime())); //���Ҫ��������   
        cal.setFirstDayOfWeek(Calendar.MONDAY);//����һ�����ڵĵ�һ�죬���й���ϰ��һ�����ڵĵ�һ��������һ   
        int day = cal.get(Calendar.DAY_OF_WEEK);//��õ�ǰ������һ�����ڵĵڼ���   
        cal.add(Calendar.DATE, cal.getFirstDayOfWeek()-day-7+dayNum);//���������Ĺ��򣬸���ǰ���ڼ�ȥ���ڼ���һ�����ڵ�һ��Ĳ�ֵ    
        String imptimeBegin = sdf.format(cal.getTime());   
        return imptimeBegin;
           
    }  
	
	
	/**
	 * ��ȡ��ǰʱ��
	 * @param args
	 */
	
	public static String getNowDate(){
		SimpleDateFormat sdf=new SimpleDateFormat("yyyyMMdd"); //����ʱ���ʽ   
		Calendar cal = Calendar.getInstance(); 
		String nowDate=sdf.format(cal.getTime());
		
		return nowDate;
	}
	

	/**
	 * �ж�һ���ַ����Ƿ������֣�����С��
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
	 * ����ܼ�  ��i=0 ��õ������ܼ� i=-1 ����������ܼ� ��i=-2���ǰ̨���ܼ� 
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
