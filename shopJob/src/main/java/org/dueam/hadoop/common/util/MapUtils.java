package org.dueam.hadoop.common.util;

import org.apache.commons.io.filefilter.SizeFileFilter;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.SerializationUtils;
import org.apache.commons.lang.StringUtils;

import java.io.*;
import java.util.*;

/**
 * User: windonly
 * Date: 10-12-20 ÏÂÎç5:49
 */
public class MapUtils implements Commons {
    public static Map<String, List<String[]>> map(List<? extends Object> lines) {
        Map<String, List<String[]>> map = new HashMap<String, List<String[]>>();
        for (Object line : lines) {
            String[] tokens = null;
            if (line instanceof String) {
                tokens = StringUtils.splitPreserveAllTokens((String) line, TAB);
            } else {
                tokens = (String[]) line;
            }
            String key = tokens[0];
            String[] value = Utils.subArray(tokens, 1);
            List<String[]> values = map.get(key);
            if (values == null) {
                values = new ArrayList<String[]>();
                map.put(key, values);
            }
            values.add(value);
        }

        return map;
    }

    public static Map<String, List<String[]>> map(List<? extends Object> lines,char splitChar) {
        Map<String, List<String[]>> map = new HashMap<String, List<String[]>>();
        for (Object line : lines) {
            String[] tokens = null;
            if (line instanceof String) {
                tokens = StringUtils.splitPreserveAllTokens((String) line, splitChar);
            } else {
                tokens = (String[]) line;
            }
            String key = tokens[0];
            String[] value = Utils.subArray(tokens, 1);
            List<String[]> values = map.get(key);
            if (values == null) {
                values = new ArrayList<String[]>();
                map.put(key, values);
            }
            values.add(value);
        }

        return map;
    }

    public static Map<String, String> toSimple(List<? extends Object> lines) {
        Map<String, String> map = new HashMap<String, String>();
        for (Object line : lines) {
            String[] tokens = null;
            if (line instanceof String) {
                tokens = StringUtils.splitPreserveAllTokens((String) line, TAB);
            } else {
                tokens = (String[]) line;
            }
            if (tokens.length > 1) {
                String key = tokens[0];
                String value = tokens[1];
                map.put(key, value);
            }
        }

        return map;
    }


    public static Map<String, String[]> toMap(List<String[]> lines) {
        Map<String, String[]> map = new LinkedHashMap<String, String[]>();
        if (lines == null) return map;
        for (Object line : lines) {
            String[] tokens = null;
            if (line instanceof String) {
                tokens = StringUtils.splitPreserveAllTokens((String) line, TAB);
            } else {
                tokens = (String[]) line;
            }
            String key = tokens[0];
            String[] value = Utils.subArray(tokens, 1);
            if (map.containsKey(key)) {
                 String[] value1 = map.get(key);
                 map.put(key,combinArray(value,value1)) ;
            } else{
                map.put(key, value);
            }

        }

        return map;
    }

    private  static String[] combinArray(String[] value1, String[] value2) {
        int size1 = value1.length, size2 = value2.length;
        if (size1 >= size2) {
            for (int i = 0; i < size2; i++) {
                value1[i] =String.valueOf(Long.parseLong(value1[i]) + Long.parseLong(value2[i]));
            }
            return value1;
        } else {
            for (int i = 0; i < size1; i++) {
                value2[i] = String.valueOf(Long.parseLong(value1[i]) + Long.parseLong(value2[i]));
            }
            return value2;
        }
    }


    public static Map<String, String> toSimpleMap(List<String[]> lines) {
        Map<String, String> map = new LinkedHashMap<String, String>();
        if (lines == null) return map;
        for (String[] line : lines) {
            String[] tokens = line;
            String key = tokens[0];
            String value = tokens[1];
            map.put(key, value);
        }

        return map;
    }


    public static Map<String, String> asMap(String[] input) {
        Map<String, String> map = new LinkedHashMap<String, String>();
        for (int i = 0; i < input.length; i = i + 2) {
            map.put(input[i], input[i + 1]);
        }
        return map;
    }

    /**
     * ´¦Àí a=b&c=d
     *
     * @param input
     * @return
     */
    public static Map<String, String> asMap(String input) {
        Map<String, String> map = new HashMap<String, String>();
        String[] allCols = StringUtils.splitPreserveAllTokens(input, '&');
        for (String line : allCols) {
            if (line.indexOf('=') > 0) {
                map.put(line.substring(0, line.indexOf('=')), line.substring(line.indexOf('=') + 1));
            }
        }
        return map;
    }

    public static void main(String[] args) {
        System.out.println(asMap("a=b&c=d&e=&=b"));
    }
}
