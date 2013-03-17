package org.dueam.hadoop.conf;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * User: windonly
 * Date: 11-1-6 ÏÂÎç6:22
 */
public class Cart {
    public static long[] area = new long[]{0, 60, 60 * 5, 60 * 10, 60 * 30, 60 * 60 * 1, 60 * 60 * 2, 60 * 60 * 12, 60 * 60 * 24 * 1,60 * 60 * 24 * 2,60 * 60 * 24 * 4,60 * 60 * 24 * 7,60 * 60 * 24 * 14,60 * 60 * 24 * 30};
    public static String[] areaNames = new String[]{"0", "1min", "5min", "10min", "30min", "1h", "2h", "12h", "24h","2d","4d","7d","14d","30d"};
    public static List<String> areaKeys = new ArrayList<String>();

    static {
        for (long _value : area) {
            areaKeys.add(getArea(_value + 1));
        }
    }

    public static String getArea(long value) {
        int pos = 0;
        for (long area_value : area) {
            if (value < area_value)
                return "[" + ("24h".equals(areaNames[pos - 1])?"1d":areaNames[pos - 1]) + "-" + areaNames[pos] + "]";
            pos++;
        }
        return "[" + areaNames[area.length - 1] + "-max]";
    }
    public static void  main(String[] args){
        System.out.println(area.length == areaNames.length);
        System.out.println(areaKeys);
    }
}
