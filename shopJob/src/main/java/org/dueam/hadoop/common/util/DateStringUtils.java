package org.dueam.hadoop.common.util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * deal date strings
 * User: windonly
 * Date: 10-12-10 下午4:40
 */
public class DateStringUtils {
    /**
     * a default date format as yyyyMMdd
     */
    public final static String DEFAULT_DATE_STYLE = "yyyyMMdd";
    public final static String DEFAULT_DATETIME_STYLE = "yyyy-MM-dd HH:mm:ss";

    /**
     * return yesterday (as hadoop only has yesterday's data)
     *
     * @return
     */
    public static String now(String style) {
        Calendar c = Calendar.getInstance();
        c.setTime(new Date());
        c.add(Calendar.DAY_OF_YEAR,-1);
        return format(c.getTime(),style);
    }

    public static String add(String date,int day) {
        Calendar c = Calendar.getInstance();
        c.setTime(parse(date,DEFAULT_DATE_STYLE));
        c.add(Calendar.DAY_OF_YEAR,day);
        return format(c.getTime(),DEFAULT_DATE_STYLE);
    }

    public static String add(String date,int day,String style) {
        Calendar c = Calendar.getInstance();
        c.setTime(parse(date,style));
        c.add(Calendar.DAY_OF_YEAR,day);
        return format(c.getTime(),style);
    }




    public static long second(String start,String end) {
        Date startTime = parse(start,DEFAULT_DATETIME_STYLE);
        Date endTime = parse(end,DEFAULT_DATETIME_STYLE);
        return (endTime.getTime() - startTime.getTime() ) / 1000;
    }

    public static long second(String start,String end,String formatStyle) {
        Date startTime = parse(start,formatStyle);
        Date endTime = parse(end,formatStyle);
        return (endTime.getTime() - startTime.getTime() ) / 1000;
    }

    /**
     * 返回当前时间
     * @return
     */
    public static String now(){
        return now(DEFAULT_DATE_STYLE);
    }

    /**
     * format date
     * @param date
     * @param style format stye
     * @return
     */
    public static String format(Date date, String style) {
        SimpleDateFormat df = new SimpleDateFormat(style);
        return df.format(date);
    }

    public static Date parse(String date, String style) {
        try {
            SimpleDateFormat df = new SimpleDateFormat(style);
            return df.parse(date);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * format date style from yyyyMMdd to yyyy-MM-dd
     * @param date
     * @return
     */
    public static String format(String date){
        return date.substring(0, 4) + "-" + date.substring(4, 6) + "-"
					+ date.substring(6, 8);
    }

    /**
     * 截取日期数据 (yyyy-MM-dd)
     * @param date 日期（yyyy-MM-dd HH:mm:ss)
     * @return
     */
    public static String getDate(String date) {
        if (date.length() < 10) return date;
        return date.substring(0, 10);

    }
}
