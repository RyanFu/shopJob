package org.dueam.hadoop.jobs;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.LongSumReducer;
import org.apache.hadoop.util.ToolRunner;
import org.dueam.hadoop.common.Area;
import org.dueam.hadoop.common.HadoopTable;
import org.dueam.hadoop.common.SimpleMapReduce;
import org.dueam.hadoop.common.tables.TcBizOrder;
import org.dueam.hadoop.common.util.DateStringUtils;
import org.dueam.hadoop.common.util.Utils;

import java.io.IOException;
import java.util.Iterator;

/**
 * 交易中心报表 (TOP)
 * User: windonly
 * Date: 10-12-16 下午5:44
 */
public class TradeTopMonitor extends SimpleMapReduce {
    @Override
    protected String[] getInputPath(String[] args) {
        String[] input = HadoopTable.orderDelta(args[0]).getInputPath();
        System.out.println("input path => " + Utils.asString(input));
        return input;
    }

    @Override
    protected void beforeRun(String[] args, JobConf config) {
        config.setCombinerClass(LongSumReducer.class);
    }

    @Override
    protected Class getInputFormat() {
        return HadoopTable.orderDelta(null).getInputFormat();
    }

    @Override
    protected String getOutputPath(String[] args) {
        return "/group/tbdev/xiaodu/suoni/trade_top_monitor/" + args[0];

    }

    public void reduce(Text key, Iterator<LongWritable> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        long sum = 0;
        while (values.hasNext()) {
            sum += values.next().get();
        }
        //if sum lt 10 , return it
        if (sum < 10) return;
        //日交易额小于1000就不要了
        if(key.toString().startsWith("seller") && sum < 1000 * 100){
           return;
        }
         //日交易额小于1000的买家就不要了
        if(key.toString().startsWith("buyer") && sum < 1000 * 100){
           return;
        }
         //日成交笔数小于10的买家就不要了
        if(key.toString().startsWith("tn_buyer") && sum < 10){
           return;
        }

        StringBuffer _tmp = new StringBuffer();
        _tmp.append(sum).append(TAB);
        output.collect(key, new Text(_tmp.toString()));
    }

    @Override
    protected void doWork(String line, OutputCollector<Text, LongWritable> output) throws IOException {
        String[] _allCols = StringUtils.splitPreserveAllTokens(line, TAB);
        if (_allCols.length < 47) {
            return;
        }
        String queryDate = DateStringUtils.format(inputArgs[0]);
        String gmtCreated = _allCols[TcBizOrder.GMT_CREATE];
        String payTime = _allCols[TcBizOrder.PAY_TIME];
        String gmtModified = _allCols[TcBizOrder.GMT_MODIFIED];


        // filter not effective order
        if (!TcBizOrder.isEffective(_allCols)) {
            return;
        }

        //处理小订单 TOP 100卖家
        if (TcBizOrder.isMain(_allCols)) {
            if (Utils.isSameDay(payTime, queryDate)) {
                long fee = TcBizOrder.getTotalFee(_allCols);
                if (fee < 100) {
                    output.collect(Utils.mergeKey("small_order", _allCols[TcBizOrder.SELLER_ID]), ONE);
                }
                //根据卖家汇总
                LongWritable totalFee = new LongWritable(fee);
                output.collect(Utils.mergeKey("seller", "all", _allCols[TcBizOrder.SELLER_ID]), totalFee);
                if (TcBizOrder.isB2C(_allCols)) {
                    output.collect(Utils.mergeKey("seller", "b2c", _allCols[TcBizOrder.SELLER_ID]), totalFee);
                } else {
                    output.collect(Utils.mergeKey("seller", "c2c", _allCols[TcBizOrder.SELLER_ID]), totalFee);
                }

                if (TcBizOrder.isFromTgroupon(_allCols)) {
                    output.collect(Utils.mergeKey("seller", "jhs", _allCols[TcBizOrder.SELLER_ID]), totalFee);
                }
            }
        }

        //处理小订单 TOP 100买家
        if (TcBizOrder.isMain(_allCols)) {
            if (Utils.isSameDay(payTime, queryDate)) {
                long fee = TcBizOrder.getTotalFee(_allCols);
                //根据卖家汇总
                LongWritable totalFee = new LongWritable(fee);
                output.collect(Utils.mergeKey("buyer", "all", _allCols[TcBizOrder.BUYER_ID]), totalFee);
            }
        }

         //处理小订单 TOP 100买家  笔数
        if (TcBizOrder.isDetail(_allCols)) {
            if (Utils.isSameDay(payTime, queryDate)) {
                output.collect(Utils.mergeKey("tn_buyer", "all", _allCols[TcBizOrder.BUYER_ID]), ONE);
            }
        }

        if (TcBizOrder.isDetail(_allCols) && Utils.isSameDay(payTime, queryDate)) {
            output.collect(Utils.mergeKey("auction", "all", _allCols[TcBizOrder.AUCTION_ID]), ONE);
            if(TcBizOrder.isB2C(_allCols)){
                output.collect(Utils.mergeKey("auction","b2c", _allCols[TcBizOrder.AUCTION_ID]), ONE);
            } else {
                output.collect(Utils.mergeKey("auction","c2c", _allCols[TcBizOrder.AUCTION_ID]), ONE);
            }

            if(TcBizOrder.isFromTgroupon(_allCols)){
                  output.collect(Utils.mergeKey("auction","jhs", _allCols[TcBizOrder.AUCTION_ID]), ONE);
            }
        }


    }


    public static String getMinutes(String time) {
        return StringUtils.substring(time, 0, 16);
    }

    private static void commonTotalMonitor(String firstKey, String secondKey, String[] _allCols, OutputCollector<Text, LongWritable> output, String queryDate) throws IOException {
        String gmtCreated = _allCols[TcBizOrder.GMT_CREATE];
        String payTime = _allCols[TcBizOrder.PAY_TIME];
        String gmtModified = _allCols[TcBizOrder.GMT_MODIFIED];
        if (TcBizOrder.isDetail(_allCols)) {
            long fee = TcBizOrder.getTotalFee(_allCols);
            LongWritable totalFee = new LongWritable(fee);
            //统计GMV 和支付宝交易
            if (Utils.isSameDay(gmtCreated, queryDate)) {
                output.collect(Utils.mergeKey("t", firstKey, "gmv", secondKey), totalFee);
                output.collect(Utils.mergeKey("t", firstKey, "gmv_num", secondKey), ONE);
            }
            if (Utils.isSameDay(payTime, queryDate)) {
                output.collect(Utils.mergeKey("t", firstKey, "alipay", secondKey), totalFee);
                output.collect(Utils.mergeKey("t", firstKey, "alipay_num", secondKey), ONE);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),
                new TradeTopMonitor(), args);
        System.exit(res);
    }


}
