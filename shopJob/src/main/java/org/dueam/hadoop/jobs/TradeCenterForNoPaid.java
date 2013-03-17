package org.dueam.hadoop.jobs;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.util.ToolRunner;
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
public class TradeCenterForNoPaid extends SimpleMapReduce {
    @Override
    protected String[] getInputPath(String[] args) {
        String[] input = HadoopTable.orderDelta(args[0]).getInputPath();
        System.out.println("input path => " + Utils.asString(input));
        return input;
    }

    @Override
    protected Class getInputFormat() {
        return HadoopTable.orderDelta(null).getInputFormat();
    }

    @Override
    protected String getOutputPath(String[] args) {
        return "/group/tbdev/xiaodu/suoni/trade_center_for_no_paid/" + args[0];

    }


    @Override
    protected void doWork(String line, OutputCollector<Text, LongWritable> output) throws IOException {
        String[] _allCols = StringUtils.splitPreserveAllTokens(line, TAB);
        if (_allCols.length < 47) {
            return;
        }

        // filter not effective order
        if (!TcBizOrder.isEffective(_allCols)) {
            return;
        }
        String queryDate = DateStringUtils.format(inputArgs[0]);
        String gmtCreated = _allCols[TcBizOrder.GMT_CREATE];
        String payTime = _allCols[TcBizOrder.PAY_TIME];
        String gmtModified = _allCols[TcBizOrder.GMT_MODIFIED];
        String payStatus = _allCols[TcBizOrder.PAY_STATUS];
        if (!"1".equals(payStatus)) return;

        //今天创建的且还未付款的订单统计
        if (Utils.isSameDay(gmtCreated, queryDate)) {

            //group by biz type ( gmv trade )
            if (TcBizOrder.isMain(_allCols)) {
                //group by biz type
                output.collect(new Text("bizType" + TAB + _allCols[TcBizOrder.BIZ_TYPE]), new LongWritable(TcBizOrder.getTotalFee(_allCols)));
            }
            //group by root category ( gmv trade )
            if (TcBizOrder.isDetail(_allCols)) {
                output.collect(new Text("cat" + TAB + Utils.getValue(_allCols[TcBizOrder.ATTRIBUTES], "realRootCat", "NULL")), new LongWritable(TcBizOrder.getTotalFee(_allCols)));
            }

        }

        //统计订单笔数
        if (Utils.isSameDay(gmtCreated, queryDate)) {

            //group by biz type ( gmv trade )
            if (TcBizOrder.isDetail(_allCols)) {
                //group by biz type
                output.collect(new Text("bizTypeNum" + TAB + _allCols[TcBizOrder.BIZ_TYPE]), ONE);
            }
            //group by root category ( gmv trade )
            if (TcBizOrder.isDetail(_allCols)) {
                output.collect(new Text("catNum" + TAB + Utils.getValue(_allCols[TcBizOrder.ATTRIBUTES], "realRootCat", "NULL")), ONE);
            }

        }

        //group by create order
        if (Utils.isSameDay(gmtCreated, queryDate) && TcBizOrder.isMain(_allCols) && TcBizOrder.isEffective(_allCols)) {
            String type = "gmv";
            commonTrade(type, _allCols, output);
        }

        //group by seller
        if (Utils.isSameDay(gmtCreated, queryDate) && TcBizOrder.isDetail(_allCols)) {
            LongWritable totalFee = new LongWritable(TcBizOrder.getTotalFee(_allCols));
            output.collect(Utils.mergeKey("seller", _allCols[TcBizOrder.SELLER_ID]), totalFee);
        }

        //group by seller
        if (Utils.isSameDay(gmtCreated, queryDate) && TcBizOrder.isDetail(_allCols)) {
            LongWritable totalFee = new LongWritable(TcBizOrder.getTotalFee(_allCols));
            output.collect(Utils.mergeKey("buyer", _allCols[TcBizOrder.BUYER_ID]), totalFee);
        }

    }


    private static void commonTrade(String type, String[] _allCols, OutputCollector<Text, LongWritable> output) throws IOException {
        long fee = TcBizOrder.getTotalFee(_allCols);
        LongWritable totalFee = new LongWritable(fee);

        output.collect(Utils.mergeKey(type, "all"), totalFee);
        if (TcBizOrder.isB2C(_allCols)) {
            //b2c
            output.collect(Utils.mergeKey(type, "b2c"), totalFee);
        } else {
            //c2c
            output.collect(Utils.mergeKey(type, "c2c"), totalFee);
        }

        if (TcBizOrder.isFromTgroupon(_allCols)) {
            output.collect(Utils.mergeKey(type, "jhs"), totalFee);
        }
        if ("1".equals(Utils.getValue(_allCols[TcBizOrder.ATTRIBUTES], "wap", "0"))) {
            //wap
            output.collect(Utils.mergeKey(type, "wap"), totalFee);
        }
        //trade type as 1，一口价  2，拍卖  3，团购
        output.collect(Utils.mergeKey(type, TcBizOrder.getSubType(_allCols)), totalFee);

        if ("1".equals(Utils.getValue(_allCols[TcBizOrder.ATTRIBUTES], "fbuy", "0"))) {
            //fast buy ( guest )
            output.collect(Utils.mergeKey(type, "fbuy"), totalFee);
        }

        if (NumberUtils.toLong(Utils.getValue(_allCols[TcBizOrder.ATTRIBUTES], "bbcid", "0"), 0) > 0) {
            //dpc
            String[] tags = StringUtils.split(Utils.getValue(_allCols[TcBizOrder.ATTRIBUTES], "itemTag", ""), ',');
            if (ArrayUtils.contains(tags, "450")) {
                output.collect(Utils.mergeKey(type, "dpc_jx"), totalFee);
            } else {
                output.collect(Utils.mergeKey(type, "dpc_dx"), totalFee);
            }
        }

        if ("1".equals(Utils.getValue(_allCols[TcBizOrder.ATTRIBUTES], "cart", "0"))) {
            //order from cart
            output.collect(Utils.mergeKey(type, "cart"), totalFee);
        }

        if ("1".equals(Utils.getValue(_allCols[TcBizOrder.ATTRIBUTES], "top", "0"))) {
            //order from top
            output.collect(Utils.mergeKey(type, "top"), totalFee);
        }
    }


    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),
                new TradeCenterForNoPaid(), args);
        System.exit(res);
    }


}
