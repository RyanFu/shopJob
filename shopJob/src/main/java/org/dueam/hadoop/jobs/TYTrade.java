package org.dueam.hadoop.jobs;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.LongSumReducer;
import org.apache.hadoop.util.ToolRunner;
import org.dueam.hadoop.common.Area;
import org.dueam.hadoop.common.CounterMap;
import org.dueam.hadoop.common.HadoopTable;
import org.dueam.hadoop.common.MapReduce;
import org.dueam.hadoop.common.tables.TcBizOrder;
import org.dueam.hadoop.common.util.Utils;

import java.io.IOException;
import java.util.Iterator;

/**
 * 用户交易监控报表
 * User: windonly
 * Date: 10-12-16 下午5:44
 */
public class TYTrade extends MapReduce<Text, LongWritable> {
    @Override
    protected void setup(String[] args) {
        this.taskNum = 200;
        this.input = HadoopTable.order(args[0], "200").getInputPath();
        this.inputFormat = HadoopTable.order(null).getInputFormat();
        this.output = "/group/tbbp/suoni/ty_trade/" + args[0];
    }

    @Override
    protected void beforeRun(String[] args, JobConf config) {
        config.setMapOutputValueClass(LongWritable.class);
        config.setCombinerClass(LongSumReducer.class);
        config.setReducerClass(LongSumReducer.class);
    }

    @Override
    public void reduce(Text key, Iterator<LongWritable> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

    }

    @Override
    public void map(Object key, Text value, OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException {
        String[] _allCols = StringUtils.splitPreserveAllTokens(value.toString(), TAB);
        if (_allCols.length < 47) {
            return;
        }

        if (TcBizOrder.isPaied(_allCols) && TcBizOrder.isDetail(_allCols)) {
            String buyerId = _allCols[TcBizOrder.BUYER_ID];
            output.collect(new Text(buyerId), ONE);
        }
    }


    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),
                new TYTrade(), args);
        System.exit(res);
    }


}
