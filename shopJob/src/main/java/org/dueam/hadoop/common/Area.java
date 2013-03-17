package org.dueam.hadoop.common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 区间统计工具类
 * User: windonly
 * Date: 11-5-6 下午1:25
 */
public class Area {
    public static interface KeyNameCallback {
        public String callback(long value);
    }

    public static KeyNameCallback MoneyCallback = new KeyNameCallback() {

        public String callback(long value) {
            return String.valueOf(value / 100);
        }
    };

    public static KeyNameCallback TimeCallback = new KeyNameCallback() {

        public String callback(long value) {
            if(value < 60){
                return value +"S";
            }else if(value < 3600){
                return value/60 +"S";
            } else if(value < 24 * 3600){
                return value/3600 +"H";
            } else{
               return value/(3600*24) +"D";
            }
        }
    };
    private KeyNameCallback callback = null;
    public long[] area = new long[]{1, 2, 5, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 120, 140, 160, 180, 200, 500, 1000, 2000};

    public List<String> areaKeys = new ArrayList<String>();

    public Area(long[] area) {
        this.area = area;
        init();
    }

    public Area(long[] area, KeyNameCallback callback) {
        this.area = area;
        this.callback = callback;
        init(callback);
    }

    public static Area newArea(long... area) {
        return new Area(area);
    }

    public static Area newArea(KeyNameCallback callback, long... area) {
        return new Area(area, callback);
    }

    private void init() {
        areaKeys.clear();
        for (long _value : area) {
            areaKeys.add(getArea(_value));
        }
    }

    private void init(KeyNameCallback callback) {
        areaKeys.clear();
        for (long _value : area) {
            areaKeys.add(getArea(_value, callback));
        }
    }

    public String getArea(long value) {
        if(null != callback){
            return getArea(value,callback);
        }
        int pos = 0;
        for (long area_value : area) {
            if (value < area_value)
                return "[" + area[pos - 1] + "-" + (area[pos]) + ")";
            pos++;
        }
        return "[" + area[area.length - 1] + "-max]";
    }

    public String getArea(long value, KeyNameCallback callback) {
        int pos = 0;
        for (long area_value : area) {
            if (value < area_value)
                return "[" + callback.callback(area[pos - 1]) + "-" + callback.callback((area[pos])) + ")";
            pos++;
        }
        return "[" + callback.callback(area[area.length - 1]) + "-max]";
    }

    public String getArea(double value) {
        int pos = 0;
        for (long area_value : area) {
            if (value < area_value)
                return "[" + area[pos - 1] + "-" + (area[pos]) + ")";
            pos++;
        }
        return "[" + area[area.length - 1] + "-max]";
    }

    private Map<String, Long> countMap = null;

    public void count(double value) {
        if (countMap == null) {
            countMap = new HashMap<String, Long>();
        }
        String key = getArea(value);
        Long oldCount = countMap.get(key);
        if (oldCount == null) {
            oldCount = 0L;
        }
        oldCount++;
        countMap.put(key, oldCount);
    }

    /**
     *  统计总金额占比
     * @param value
     */
    public void sum(long value) {
        if (countMap == null) {
            countMap = new HashMap<String, Long>();
        }
        String key = getArea(value);
        Long oldCount = countMap.get(key);
        if (oldCount == null) {
            oldCount = 0L;
        }
        oldCount = oldCount + value;
        countMap.put(key, oldCount);
    }

    public Long get(String key) {
        return countMap.get(key);
    }
}
