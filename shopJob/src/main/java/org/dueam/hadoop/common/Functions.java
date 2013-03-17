package org.dueam.hadoop.common;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;

import java.security.Key;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 常用方法
 * User: windonly
 * Date: 11-8-11 下午5:08
 */
public class Functions extends NumberUtils {
    /**
     * to string
     *
     * @param value
     * @return
     */
    public static String str(long value) {
        return String.valueOf(value);
    }

    public static String str(int value) {
        return String.valueOf(value);
    }

    public static String str(double value) {
        return String.valueOf(value);
    }

    public static String str(boolean value) {
        return String.valueOf(value);
    }

    public static <E> ArrayList<E> newArrayList() {
        return new ArrayList<E>();
    }

    public static void print(List< ? extends Object> objects) {
        for(Object object : objects) {
            System.out.println(object);
        }
    }

    public static <K,V> Map<K,V> newHashMap(){
        return new HashMap<K,V>();
    }

}
