package org.dueam.hadoop.common;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;

import java.io.IOException;

/**
 * �ص�Map����
 * User: windonly
 * Date: 11-5-19 ����9:03
 */
public interface MapCallback {
    public void callback(Text value, OutputCollector<Text, LongWritable> output) throws IOException;
}
