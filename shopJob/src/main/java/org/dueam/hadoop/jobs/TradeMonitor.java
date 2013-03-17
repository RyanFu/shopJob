package org.dueam.hadoop.jobs;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.lib.LongSumReducer;
import org.apache.hadoop.util.ToolRunner;
import org.dueam.hadoop.common.Area;
import org.dueam.hadoop.common.HadoopTable;
import org.dueam.hadoop.common.SimpleMapReduce;
import org.dueam.hadoop.common.tables.TcBizOrder;
import org.dueam.hadoop.common.util.DateStringUtils;
import org.dueam.hadoop.common.util.Utils;

import java.io.IOException;

/**
 * 交易中心报表
 * User: windonly
 * Date: 10-12-16 下午5:44
 */
public class TradeMonitor extends SimpleMapReduce {
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
        return "/group/tbdev/xiaodu/suoni/trade_monitor/" + args[0];

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
        if (Utils.isSameDay(queryDate, gmtCreated)) {
            output.collect(Utils.mergeKey("t", "sys", "created"), ONE);
            output.collect(Utils.mergeKey("d", "sys", "created", "m", getMinutes(gmtCreated)), ONE);
            output.collect(Utils.mergeKey("d", "sys", "created", "s", gmtCreated), ONE);
        }
        if (Utils.isSameDay(queryDate, gmtModified)) {
            output.collect(Utils.mergeKey("t", "sys", "modified"), ONE);
        }

        // filter not effective order
        if (!TcBizOrder.isEffective(_allCols)) {
            return;
        }

        if (TcBizOrder.isMain(_allCols)) {
            commonMonitor("all", _allCols, output, queryDate);
            if (TcBizOrder.isB2C(_allCols)) {
                commonMonitor("b2c", _allCols, output, queryDate);
            } else {
                commonMonitor("c2c", _allCols, output, queryDate);
            }
            if (TcBizOrder.isFromTgroupon(_allCols)) {
                commonMonitor("jhs", _allCols, output, queryDate);
            }
            if (Utils.isSameDay(payTime, queryDate)) {
                long fee = TcBizOrder.getTotalFee(_allCols);
                String key = alipayTradeArea.getArea(fee);
                output.collect(Utils.mergeKey("t","alipay_area",key),ONE);
            }

        }
        if (TcBizOrder.isDetail(_allCols)) {
            String rootCatId = Utils.getValue(_allCols[TcBizOrder.ATTRIBUTES], "realRootCat", "NULL");
            commonTotalMonitor("cat", rootCatId, _allCols, output, queryDate);
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

    /**
     * 统计 Gmv alipay 交易金额和笔数（大额订单不做处理）以及每分钟的订单数
     *
     * @param firstKey
     * @param _allCols
     * @param output
     * @param queryDate
     * @throws IOException
     */
    private static void commonMonitor(String firstKey, String[] _allCols, OutputCollector<Text, LongWritable> output, String queryDate) throws IOException {
        String gmtCreated = _allCols[TcBizOrder.GMT_CREATE];
        String payTime = _allCols[TcBizOrder.PAY_TIME];
        String gmtModified = _allCols[TcBizOrder.GMT_MODIFIED];
        if (TcBizOrder.isMain(_allCols)) {
            long fee = TcBizOrder.getTotalFee(_allCols);
            LongWritable totalFee = new LongWritable(fee);
            //统计GMV 和支付宝交易
            if (Utils.isSameDay(gmtCreated, queryDate)) {
                output.collect(Utils.mergeKey("t", firstKey, "gmv"), totalFee);
                output.collect(Utils.mergeKey("t", firstKey, "gmv_num"), ONE);
                output.collect(Utils.mergeKey("d", firstKey, "m", "gmv", getMinutes(gmtCreated)), totalFee);
                output.collect(Utils.mergeKey("d", firstKey, "m", "gmv_num", getMinutes(gmtCreated)), ONE);
                output.collect(Utils.mergeKey("d", firstKey, "s", "gmv", gmtCreated), totalFee);
                output.collect(Utils.mergeKey("d", firstKey, "s", "gmv_num", gmtCreated), ONE);
            }
            if (Utils.isSameDay(payTime, queryDate)) {
                output.collect(Utils.mergeKey("t", firstKey, "alipay"), totalFee);
                output.collect(Utils.mergeKey("t", firstKey, "alipay_num"), ONE);
                output.collect(Utils.mergeKey("d", firstKey, "m", "alipay", getMinutes(payTime)), totalFee);
                output.collect(Utils.mergeKey("d", firstKey, "m", "alipay_num", getMinutes(payTime)), ONE);
                output.collect(Utils.mergeKey("d", firstKey, "s", "alipay", payTime), totalFee);
                output.collect(Utils.mergeKey("d", firstKey, "s", "alipay_num", payTime), ONE);
            }
        }
    }

    static Area alipayTradeArea = Area.newArea(Area.MoneyCallback, 0, 1 * 100, 10 * 100, 50 * 100, 100 * 100, 200 * 100, 300 * 100, 500 * 100, 10000 * 100, 100000 * 100);


    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),
                new TradeMonitor(), args);
        System.exit(res);
    }


}
