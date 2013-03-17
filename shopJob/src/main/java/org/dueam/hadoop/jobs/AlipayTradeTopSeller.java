package org.dueam.hadoop.jobs;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.ToolRunner;
import org.dueam.hadoop.common.CounterMap;
import org.dueam.hadoop.common.HadoopTable;
import org.dueam.hadoop.common.MapReduce;
import org.dueam.hadoop.common.tables.TcBizOrder;
import org.dueam.hadoop.common.util.DateStringUtils;
import org.dueam.hadoop.common.util.Utils;

import java.io.IOException;
import java.util.Iterator;

/**
 * 每日支付宝交易额Top卖家
 * User: windonly
 * Date: 10-12-16 下午5:44
 */
public class AlipayTradeTopSeller extends MapReduce<Text, Text> {

    @Override
    protected void setup(String[] args) {
        this.input = HadoopTable.orderDelta(args[0]).getInputPath();
        this.output = "/group/tbdev/xiaodu/suoni/alipay_trade_top_seller/" + args[0];
        this.inputFormat = HadoopTable.orderDelta(null).getInputFormat();
        this.taskNum = 100;
    }

    @Override
    protected void beforeRun(String[] args, JobConf config) {
        config.setCombinerClass(CombinerClass.class);
        config.setMapOutputValueClass(Text.class);

    }

    public static class CombinerClass extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            long feeSum = 0, countSum = 0, b2c = 0;
            CounterMap catMap = CounterMap.newCounter("Cat Counter");
            while (values.hasNext()) {
                String[] _cols = StringUtils.splitPreserveAllTokens(values.next().toString(), TAB);
                b2c += NumberUtils.toLong(_cols[0]);
                feeSum += NumberUtils.toLong(_cols[1]);
                countSum += NumberUtils.toLong(_cols[2]);
                for (String line : StringUtils.split(_cols[3], ';')) {
                    String[] array = StringUtils.split(line, ':');
                    if (array.length > 1) {
                        catMap.add(array[0], NumberUtils.toLong(array[1]));
                    }
                }
            }
            output.collect(key, Utils.mergeKey(String.valueOf(b2c),String.valueOf(feeSum), String.valueOf(countSum), catMap.toString(false, ':', ';')));
        }
    }

    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        long feeSum = 0, countSum = 0, b2c = 0;
        CounterMap catMap = CounterMap.newCounter("Cat Counter");
        while (values.hasNext()) {
            String[] _cols = StringUtils.splitPreserveAllTokens(values.next().toString(), TAB);
            b2c += NumberUtils.toLong(_cols[0]);
            feeSum += NumberUtils.toLong(_cols[1]);
            countSum += NumberUtils.toLong(_cols[2]);
            for (String line : StringUtils.split(_cols[3], ';')) {
                String[] array = StringUtils.split(line, ':');
                if (array.length > 1) {
                    catMap.add(array[0], NumberUtils.toLong(array[1]));
                }
            }
        }
        //为减少记录数，只统计每天成交额在1W以上的卖家
        if (feeSum > 1000000) {
            output.collect(key, Utils.mergeKey(String.valueOf(b2c), String.valueOf(feeSum), String.valueOf(countSum), catMap.getMaxKey()));
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
        String payTime = _allCols[TcBizOrder.PAY_TIME];

        //&& (oldDate.compareTo(gmtCreated) < 0)
        // gmtCreated.compareTo(oldDate) >=0

        // filter not effective order
        if (TcBizOrder.isEffective(_allCols) && TcBizOrder.isMain(_allCols) && Utils.isSameDay(payTime, queryDate)) {
            long fee = TcBizOrder.getTotalFee(_allCols);        //大订单过滤
            boolean bigOrder = false;
            String catId = Utils.getValue(_allCols[TcBizOrder.ATTRIBUTES], "realRootCat", "NULL");
            if (fee > 100000 * 100) {
                if ("26".equals(catId)) {
                    if (fee > 500000 * 100) bigOrder = true;
                } else {
                    bigOrder = true;
                }
            }
            if (!bigOrder) {
                output.collect(new Text(_allCols[TcBizOrder.SELLER_ID]), Utils.mergeKey(TcBizOrder.isB2C(_allCols) ? "1" : "-1", String.valueOf(fee), "1", catId + ":" + fee));
            }

        }
    }


    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),
                new AlipayTradeTopSeller(), args);
        System.exit(res);
    }


}
