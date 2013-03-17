package org.dueam.hadoop.jobs;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.ToolRunner;
import org.dueam.hadoop.common.HadoopTable;
import org.dueam.hadoop.common.MapReduce;
import org.dueam.hadoop.common.tables.TcBizOrder;
import org.dueam.hadoop.common.util.DateStringUtils;
import org.dueam.hadoop.common.util.Utils;

import java.io.IOException;
import java.util.Iterator;

/**
 * Spu交易表
 * User: windonly
 * Date: 10-12-16 下午5:44
 */
public class SpuTradeInfo extends MapReduce<Text, Text> {
    @Override
    protected void setup(String[] args) {
        this.input = HadoopTable.orderDelta(args[0]).getInputPath();
        System.out.println("input path => " + Utils.asString(input));
        this.output = "/group/tbdev/xiaodu/suoni/spu_trade_info/" + args[0];
        this.taskNum = 50;
    }

    @Override
    protected void beforeRun(String[] args, JobConf config) {
        config.setMapOutputValueClass(Text.class);
        config.setReducerClass(IdentityReducer.class);
    }

    @Override
    public void reduce(Text text, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

    }

    @Override
    public void map(Object key, Text line, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        String[] _allCols = StringUtils.splitPreserveAllTokens(line.toString(), TAB);
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


        if (Utils.isSameDay(payTime, queryDate) && TcBizOrder.isDetail(_allCols)) {
            String auctionId = _allCols[TcBizOrder.AUCTION_ID];
            String title = _allCols[TcBizOrder.AUCTION_TITLE];
            String auctionPrice = _allCols[TcBizOrder.AUCTION_PRICE];
            String buyAmount = _allCols[TcBizOrder.BUY_AMOUNT];
            String catId = Utils.getValue(_allCols[TcBizOrder.ATTRIBUTES], "catId", "NULL");
            String rootCatId = Utils.getValue(_allCols[TcBizOrder.ATTRIBUTES], "realRootCat", "NULL");
            String spuId = Utils.getValue(_allCols[TcBizOrder.ATTRIBUTES], "spuid", "NULL");
            output.collect(Utils.mergeKey(auctionId, title, auctionPrice, buyAmount, catId, rootCatId), Utils.mergeKey(spuId));
        }
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),
                new SpuTradeInfo(), args);
        System.exit(res);
    }


}
