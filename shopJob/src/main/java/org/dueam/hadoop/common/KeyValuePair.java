package org.dueam.hadoop.common;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.math.NumberUtils;

import java.util.*;

/**
 * key-value pair£¨Key & Value£©
 * User: windonly
 * Date: 11-8-11 ÏÂÎç8:24
 */
public class KeyValuePair implements Comparable<KeyValuePair> {
    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    private String key;

    public long getValue() {
        return value;
    }

    public void setValue(long value) {
        this.value = value;
    }

    private long value;

    public long getExtValue() {
        return extValue;
    }

    public void setExtValue(long extValue) {
        this.extValue = extValue;
    }

    private long extValue;

    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }

    public int compareTo(KeyValuePair target) {
        return comparator.compare(this, target);
    }

    private KeyValuePair(String key, long value, long extValue, Comparator<KeyValuePair> comparator) {
        this.key = key;
        this.value = value;
        this.comparator = comparator;
        this.extValue = extValue;
    }

    public static KeyValuePair newPair(String key, long value, Comparator<KeyValuePair> comparator) {
        return new KeyValuePair(key, value, 0, comparator);
    }

    public static KeyValuePair newPair(String key, long value, long extValue, Comparator<KeyValuePair> comparator) {
        return new KeyValuePair(key, value, extValue, comparator);
    }

    public static KeyValuePair newPair(String key, long value, long extValue) {
        return new KeyValuePair(key, value, extValue, NumberKey);
    }

    private Comparator<KeyValuePair> comparator = null;

    public static Comparator<KeyValuePair> NumberKey = new Comparator<KeyValuePair>() {
        public int compare(KeyValuePair o1, KeyValuePair o2) {
            return (int) (NumberUtils.toLong(o1.getKey()) - NumberUtils.toDouble(o2.getKey()));
        }
    };

    public static Comparator<KeyValuePair> DateKey = new Comparator<KeyValuePair>() {
        public int compare(KeyValuePair o1, KeyValuePair o2) {
            return (int) (o1.getKey().compareTo(o2.getKey()));
        }
    };

    public static Comparator<KeyValuePair> NumberKeyDesc = new Comparator<KeyValuePair>() {
        public int compare(KeyValuePair o1, KeyValuePair o2) {
            return (int) (NumberUtils.toLong(o2.getKey()) - NumberUtils.toDouble(o1.getKey()));
        }
    };

    public static Comparator<KeyValuePair> NumberValueDesc = new Comparator<KeyValuePair>() {
        public int compare(KeyValuePair o1, KeyValuePair o2) {
            return (int) (o2.getValue() - o1.getValue());
        }
    };

    public static void main(String[] args) {
        List<KeyValuePair> pairs = new ArrayList<KeyValuePair>();
        pairs.add(newPair("2011-01-01", 2, 0, DateKey));
        pairs.add(newPair("2011-01-02", 10, 0, DateKey));
        pairs.add(newPair("2010-11-01", 3, 0, DateKey));
        pairs.add(newPair("2010-10-01", 1, 0, DateKey));
        Collections.sort(pairs);
        System.out.println(pairs);
    }
}
