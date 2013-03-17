package org.dueam.hadoop.jobs;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.util.ToolRunner;
import org.dueam.hadoop.common.HadoopTable;
import org.dueam.hadoop.common.SimpleMapReduce;
import org.dueam.hadoop.common.tables.AuctionAuctions;
import org.dueam.hadoop.common.tables.TcBizOrder;
import org.dueam.hadoop.common.util.DateStringUtils;
import org.dueam.hadoop.common.util.Utils;

import java.io.IOException;

/**
 * 拍卖宝贝监控
 * User: windonly
 * Date: 10-12-16 下午5:44
 */
public class Auctions extends SimpleMapReduce {
    @Override
    protected String[] getInputPath(String[] args) {
        String[] input = HadoopTable.auction(args[0]).getInputPath();
        System.out.println("input path => " + Utils.asString(input));
        return input;
    }

    @Override
    protected Class getInputFormat() {
        return HadoopTable.auction(null).getInputFormat();
    }

    @Override
    protected String getOutputPath(String[] args) {
        return "/group/tbdev/xiaodu/suoni/auctions/" + args[0];

    }


    @Override
    protected void doWork(String line, OutputCollector<Text, LongWritable> output) throws IOException {
        String[] _allCols = StringUtils.splitPreserveAllTokens(line, CTRL_A);
        if(_allCols.length < 51){
            return;
        }
        // filter not effective order
        if (!AuctionAuctions.isAuction(_allCols)) {
            return;
        }
//        //过滤用户或者小二删除的宝贝
//        if (!AuctionAuctions.isNormal(_allCols)) {
//            return;
//        }
        String queryDate = DateStringUtils.format(inputArgs[0]);
        long q = NumberUtils.toLong(_allCols[AuctionAuctions.QUANTITY]);

        output.collect(Utils.mergeKey("total", _allCols[AuctionAuctions.AUCTION_STATUS]), new LongWritable(1));
        if (q > 1) {
            output.collect(Utils.mergeKey("total", "many"), new LongWritable(1));
        } else {
            output.collect(Utils.mergeKey("total", "one"), new LongWritable(1));
        }

        long bid = NumberUtils.toLong(_allCols[AuctionAuctions.CURRENT_BID], 0);
        if (bid > 0) {
            output.collect(Utils.mergeKey("total", "bid"), new LongWritable(1));
        } else {
            output.collect(Utils.mergeKey("total", "unbid"), new LongWritable(1));
        }

        commons(output, _allCols, "all", queryDate);
        if (q > 1) {
            commons(output, _allCols, "many", queryDate);
        } else {
            commons(output, _allCols, "one", queryDate);
        }


    }

    public static void commons(OutputCollector<Text, LongWritable> output, String[] _allCols, String key, String queryDate) throws IOException {
        String gmtCreated = _allCols[AuctionAuctions.OLD_STARTS];
        String ends = _allCols[AuctionAuctions.ENDS];
        String starts = _allCols[AuctionAuctions.STARTS];
        long bid = NumberUtils.toLong(_allCols[AuctionAuctions.CURRENT_BID], 0);
        if (Utils.isSameDay(queryDate, gmtCreated)) {
            output.collect(Utils.mergeKey(key, "publish"), Utils.one());
        }

        if (Utils.isSameDay(queryDate, starts)) {
            output.collect(Utils.mergeKey(key, "start"), Utils.one());
        }

        if (Utils.isSameDay(queryDate, ends)) {
            output.collect(Utils.mergeKey(key, "ends"), Utils.one());
            if (bid <= 0) {
                output.collect(Utils.mergeKey(key, "ends_unbid"), Utils.one());
            } else {
                output.collect(Utils.mergeKey(key, "ends_bid"), Utils.one());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),
                new Auctions(), args);
        System.exit(res);
    }


}
