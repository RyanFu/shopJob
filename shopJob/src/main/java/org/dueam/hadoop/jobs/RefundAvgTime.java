package org.dueam.hadoop.jobs;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.SecondaryNameNode;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.LongSumReducer;
import org.apache.hadoop.util.ToolRunner;
import org.dueam.hadoop.common.HadoopTable;
import org.dueam.hadoop.common.MapReduce;
import org.dueam.hadoop.common.tables.TcRefundTrade;
import org.dueam.hadoop.common.util.DateStringUtils;
import org.dueam.hadoop.common.util.Utils;

import java.io.IOException;
import java.util.Iterator;

/**
 * 交易中心报表
 * User: windonly
 * Date: 10-12-16 下午5:44
 */
public class RefundAvgTime extends MapReduce<Text, Text> {


    @Override
    protected void setup(String[] args) {
        this.output = "/group/tbdev/xiaodu/suoni/refund_avg_time/" + args[0];
        this.inputFormat = HadoopTable.refund(args[0]).getInputFormat();
        this.input = HadoopTable.refund(args[0]).getInputPath();
    }

    @Override
    protected void beforeRun(String[] args, JobConf config) {
        config.setCombinerClass(CombinerClass.class);
        config.setMapOutputValueClass(Text.class);
        config.setReducerClass(getClass());
    }

    public static class CombinerClass extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            long second = 0, returnFee = 0, totalFee = 0, count = 0;
            while (values.hasNext()) {
                String[] cols = StringUtils.splitPreserveAllTokens(values.next().toString(), TAB);
                second += NumberUtils.toLong(cols[0]);
                returnFee += NumberUtils.toLong(cols[1]);
                totalFee += NumberUtils.toLong(cols[2]);
                count += NumberUtils.toLong(cols[3]);
            }
            output.collect(key, Utils.mergeKey(String.valueOf(second), String.valueOf(returnFee), String.valueOf(totalFee), String.valueOf(count)));
        }
    }

    @Override
    @Deprecated  //直接采用LongSumReducer了
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {


        long second = 0, returnFee = 0, totalFee = 0, count = 0;
        while (values.hasNext()) {
            String[] cols = StringUtils.splitPreserveAllTokens(values.next().toString(), TAB);
            second += NumberUtils.toLong(cols[0]);
            returnFee += NumberUtils.toLong(cols[1]);
            totalFee += NumberUtils.toLong(cols[2]);
            count += NumberUtils.toLong(cols[3]);
        }
        output.collect(key, Utils.mergeKey(String.valueOf(second), String.valueOf(returnFee), String.valueOf(totalFee), String.valueOf(count),String.valueOf(((double )second)/count)));
    }


    @Override
    public void map(Object key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        String[] _allCols = StringUtils.splitPreserveAllTokens(value.toString(), TAB);
        if (_allCols.length < 41) {
            return;
        }

        String queryDate = DateStringUtils.format(inputArgs[0]);
        String gmtCreated = _allCols[TcRefundTrade.gmt_create];
        String gmtModified = _allCols[TcRefundTrade.gmt_modified];
        if(gmtModified.compareTo("2010-10-01 00:00:00")< 0){
            return;
        }
        if ("4".equals(_allCols[TcRefundTrade.refund_status]) || "5".equals(_allCols[TcRefundTrade.refund_status])) {
            long second = DateStringUtils.second(gmtCreated, gmtModified);
            output.collect(Utils.mergeKey(gmtModified.substring(0,7), _allCols[TcRefundTrade.refund_status], _allCols[TcRefundTrade.need_return_goods]), Utils.mergeKey(String.valueOf(second), _allCols[TcRefundTrade.return_fee], _allCols[TcRefundTrade.total_fee], "1"));
        }

    }


    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),
                new RefundAvgTime(), args);
        System.exit(res);
    }


}
