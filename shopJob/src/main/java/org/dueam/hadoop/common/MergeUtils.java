package org.dueam.hadoop.common;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.lang.math.RandomUtils;

import javax.xml.crypto.dsig.keyinfo.KeyValue;
import java.security.KeyPair;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.dueam.hadoop.common.Functions.newArrayList;
import static org.dueam.hadoop.common.Functions.print;

/**
 * 合并多个Key的工具类
 * User: windonly
 * Date: 11-8-11 下午8:46
 */
public class MergeUtils {
    public static long sum(Collection<KeyValuePair> pairs) {
        long sum = 0;
        if (pairs != null) {
            for (KeyValuePair pair : pairs) {
                sum += pair.getValue();
            }
        }
        return sum;
    }

    public static List<KeyValuePair> merge(List<KeyValuePair> pairs, long mergeValue, KeyMerger merger) {
        List<KeyValuePair> returnList = new ArrayList<KeyValuePair>();
        Collections.sort(pairs);
        KeyValuePair begin = null;
        KeyValuePair end = null;
        long tmpSum = 0;
        long extSum = 0;
        for (KeyValuePair pair : pairs) {
            if (null == begin) {
                tmpSum += pair.getValue();
                extSum += pair.getExtValue();
                begin = pair;
            } else {
                if (tmpSum > mergeValue || pair.getValue() > mergeValue) {
                    returnList.add(KeyValuePair.newPair(merger.merge(begin, end, false), tmpSum,extSum));
                    tmpSum = pair.getValue();
                    extSum = pair.getExtValue();
                    begin = pair;
                    end = null;
                } else {
                    tmpSum += pair.getValue();
                    extSum += pair.getExtValue();
                    end = pair;
                }
            }
        }
        if (begin != null) {
            returnList.add(KeyValuePair.newPair(merger.merge(begin, end, true), tmpSum,extSum));
        }

        return returnList;
    }

    public static interface KeyMerger {
        public String merge(KeyValuePair o1, KeyValuePair o2, boolean isLast);
    }

    public static void main(String[] args) {
        List<KeyValuePair> pairs = newArrayList();
        for (int i = 0; i < 20; i++) {
            pairs.add(KeyValuePair.newPair(i + "", RandomUtils.nextInt(100),1));
        }
        print(pairs);
        long mergeValue = sum(pairs) / 10;
        print(merge(pairs, mergeValue, new KeyMerger() {
            public String merge(KeyValuePair o1, KeyValuePair o2, boolean isLast) {
                if (isLast) return NumberUtils.toInt(o1.getKey()) * 10 + " - max";
                StringBuffer nameBuffer = new StringBuffer();
                nameBuffer.append(NumberUtils.toInt(o1.getKey()) * 10).append(" - ");
                if (o2 == null) {
                    nameBuffer.append(NumberUtils.toInt(o1.getKey()) * 10 + 9);
                } else {
                    nameBuffer.append(NumberUtils.toInt(o2.getKey()) * 10 + 9);
                }
                return nameBuffer.toString();
            }
        }));


    }
}
