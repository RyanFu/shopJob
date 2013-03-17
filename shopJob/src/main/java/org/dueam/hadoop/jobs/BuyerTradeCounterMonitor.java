package org.dueam.hadoop.jobs;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.LongSumReducer;
import org.apache.hadoop.util.ToolRunner;
import org.dueam.hadoop.common.HadoopTable;
import org.dueam.hadoop.common.MapReduce;
import org.dueam.hadoop.common.SimpleMapReduce;
import org.dueam.hadoop.common.tables.TcBizOrder;
import org.dueam.hadoop.common.util.DateStringUtils;
import org.dueam.hadoop.common.util.Utils;

import java.io.IOException;
import java.util.Iterator;

/**
 * 买家交易笔数监控（最近90天大于10W笔）
 * User: windonly
 * Date: 10-12-16 下午5:44
 */
public class BuyerTradeCounterMonitor extends MapReduce<Text, Text> {

    @Override
    protected void setup(String[] args) {
        this.input = HadoopTable.order(args[0], "200").getInputPath();
        this.output = "/group/tbdev/xiaodu/suoni/buyer_trade_counter_monitor/" + args[0];
        this.inputFormat = HadoopTable.order(null).getInputFormat();
        this.taskNum = 100;
    }

    @Override
    protected void beforeRun(String[] args, JobConf config) {
        config.setCombinerClass(CombinerClass.class);
        config.setMapOutputValueClass(Text.class);

    }

    public static class CombinerClass extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            long sum = 0, noPaid = 0, noCom = 0, noRate = 0;
            while (values.hasNext()) {
                String[] _cols = StringUtils.splitPreserveAllTokens(values.next().toString(), TAB);
                sum += NumberUtils.toLong(_cols[0]);
                noPaid += NumberUtils.toLong(_cols[1]);
                noCom += NumberUtils.toLong(_cols[2]);
                noRate += NumberUtils.toLong(_cols[3]);
            }
            output.collect(key, Utils.mergeKey(String.valueOf(sum), String.valueOf(noPaid), String.valueOf(noCom), String.valueOf(noRate)));
        }
    }

    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        long sum = 0, noPaid = 0, noCom = 0, noRate = 0;
        while (values.hasNext()) {
            String[] _cols = StringUtils.splitPreserveAllTokens(values.next().toString(), TAB);
            sum += NumberUtils.toLong(_cols[0]);
            noPaid += NumberUtils.toLong(_cols[1]);
            noCom += NumberUtils.toLong(_cols[2]);
            noRate += NumberUtils.toLong(_cols[3]);
        }
        if (sum > 20000) {
            output.collect(key, Utils.mergeKey(String.valueOf(sum), String.valueOf(noPaid), String.valueOf(noCom), String.valueOf(noRate)));
        }
    }

    @Override
    public void map(Object key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        String[] _allCols = StringUtils.splitPreserveAllTokens(value.toString(), TAB);
        if (_allCols.length < 47) {
            return;
        }
        String queryDate = DateStringUtils.format(inputArgs[0]);
        String gmtCreated = _allCols[TcBizOrder.GMT_CREATE];

        String oldDate = DateStringUtils.add(queryDate, -90) + " 00:00:00";
        //&& (oldDate.compareTo(gmtCreated) < 0)
        // gmtCreated.compareTo(oldDate) >=0

        // filter not effective order
        if (TcBizOrder.isEffective(_allCols) && TcBizOrder.isMain(_allCols) && (gmtCreated.compareTo(oldDate) >= 0)) {
            output.collect(new Text(_allCols[TcBizOrder.BUYER_ID]), Utils.mergeKey("1", TcBizOrder.noPaid(_allCols) ? "1" : "0", TcBizOrder.noCom(_allCols) ? "1" : "0", TcBizOrder.noRate(_allCols) ? "1" : "0"));
        }
    }


    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),
                new BuyerTradeCounterMonitor(), args);
        System.exit(res);
    }


}
