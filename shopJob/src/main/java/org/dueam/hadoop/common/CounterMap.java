package org.dueam.hadoop.common;

import org.apache.hadoop.fs.shell.Count;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * User: windonly
 * Date: 11-6-14 上午11:28
 */
public class CounterMap {

    private ConcurrentMap<String, AtomicLong> counter = new ConcurrentHashMap<String, AtomicLong>();

    private CounterMap(String name) {
        this.name = name;
    }

    public static CounterMap newCounter(String name) {
        return new CounterMap(name);
    }

    /**
     * 增加一个值
     *
     * @param key
     * @param value
     * @return
     */
    public long add(String key, long value) {
        if (!counter.containsKey(key)) {
            counter.put(key, new AtomicLong(0));
        }
        return counter.get(key).addAndGet(value);
    }

    /**
     * 获取统计结果
     *
     * @return
     */
    public Set<Map.Entry<String, AtomicLong>> entrySet() {
        return counter.entrySet();
    }

    private String name;

    public String toString() {
        return toString(true, '=', (char) 0x10);
    }

    public String getMaxKey() {
        long max = 0;
        String key = null;
        for (Map.Entry<String, AtomicLong> entry : counter.entrySet()) {
            if (entry.getValue().get() > max) {
                max = entry.getValue().get();
                key = entry.getKey();
            }
        }
        return key;
    }


    public long get(String key) {
        AtomicLong value = counter.get(key);
        if (value == null) return 0;
        return value.get();
    }

    public String toString(boolean printName, char eqChar, char splitChar) {
        StringBuffer buffer = new StringBuffer();
        if (printName) {
            buffer.append(name).append(splitChar);
        }
        for (Map.Entry<String, AtomicLong> entry : counter.entrySet()) {
            buffer.append(entry.getKey()).append(eqChar).append(entry.getValue().get()).append(splitChar);
        }
        return buffer.toString();
    }
}
