package org.dueam.hadoop.jobs;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.lib.LongSumReducer;
import org.apache.hadoop.util.ToolRunner;
import org.dueam.hadoop.common.HadoopTable;
import org.dueam.hadoop.common.SimpleMapReduce;
import org.dueam.hadoop.common.tables.AuctionAuctions;
import org.dueam.hadoop.common.util.DateStringUtils;
import org.dueam.hadoop.common.util.Utils;

import java.io.IOException;

/**
 * 追踪多天新发布商品的增长情况
 */
public class MallItemCenter extends SimpleMapReduce {
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
        return "/group/tbdev/xiaodu/suoni/mall_item_center/" + args[0];

    }


    @Override
    protected void beforeRun(String[] args, JobConf config) {
        config.setCombinerClass(LongSumReducer.class);
    }

    @Override
    protected void doWork(String line, OutputCollector<Text, LongWritable> output) throws IOException {
        String[] _allCols = StringUtils.splitPreserveAllTokens(line, CTRL_A);
        if (_allCols.length < 51) {
            return;
        }

        if(!"1".equals(_allCols[AuctionAuctions.USER_TYPE])){
            return;
        }

        String queryDate = DateStringUtils.format(inputArgs[0]);
        String gmtCreated = _allCols[AuctionAuctions.OLD_STARTS];
        String gmtModified = _allCols[AuctionAuctions.GMT_MODIFIED];
        String lastModified = _allCols[AuctionAuctions.LAST_MODIFIED];
        String status = _allCols[AuctionAuctions.AUCTION_STATUS];
        String auctionType = _allCols[AuctionAuctions.AUCTION_TYPE];
        String promotedStatus = _allCols[AuctionAuctions.PROMOTED_STATUS];//橱窗推荐
        String stuffStatus = _allCols[AuctionAuctions.STUFF_STATUS];//二手情况
        String categoryId = _allCols[AuctionAuctions.CATEGORY];

        output.collect(Utils.mergeKey("db_status", status), ONE);  //整个DB里的商品状态分布
        if (Utils.isSameDay(queryDate, gmtCreated)) {
            output.collect(Utils.mergeKey("total", "total_today"), ONE); //今天发布的商品
            output.collect(Utils.mergeKey("total", "today_at" + auctionType), ONE);  //汇总信息发布商品的类型分布
            output.collect(Utils.mergeKey("today_status", status), ONE);  //今天发布的商品状态分布

            if ("1".equals(promotedStatus) || "2".equals(promotedStatus)) {
                output.collect(Utils.mergeKey("total", "today_ps" + promotedStatus), ONE);//全网商品
            }
            output.collect(Utils.mergeKey("total", "today_ss" + stuffStatus), ONE);//全网商品的二手状况

            totalCount(_allCols,"today",output);
        }

        output.collect(Utils.mergeKey("total", "total_all"), ONE);  //全网商品的数量
        totalCount(_allCols,"total",output);

        output.collect(Utils.mergeKey("total", "db_at" + auctionType), ONE);//全网商品的类型分布
        if ("1".equals(promotedStatus) || "2".equals(promotedStatus)) {
            output.collect(Utils.mergeKey("total", "db_ps" + promotedStatus), ONE);//全网商品
        }
        output.collect(Utils.mergeKey("total", "db_ss" + stuffStatus), ONE);//全网商品的二手状况

        if (Utils.isSameDay(queryDate, gmtModified)) {
            output.collect(Utils.mergeKey("total", "today_modified"), ONE);
        }

        if (Utils.isSameDay(queryDate, lastModified)) {
            output.collect(Utils.mergeKey("total", "today_last_modified"), ONE);
        }
        //类目发布
        output.collect(Utils.mergeKey("db_category", categoryId), ONE);


    }

    public static void totalCount(String[] _allCols, String pre, OutputCollector<Text, LongWritable> output) throws IOException {
        if ("1".equals(_allCols[AuctionAuctions.USER_TYPE])) {
            output.collect(Utils.mergeKey("total", pre + "_b"), ONE);  //B卖家发布的商品
        } else {
            output.collect(Utils.mergeKey("total", pre + "_c"), ONE);  //C卖家发布的商品
        }
        long options = NumberUtils.toLong(_allCols[AuctionAuctions.OPTIONS], 0);
        if ((options & 4398046511104L) > 0) {
            output.collect(Utils.mergeKey("total", pre + "_hitao"), ONE);  //Hitao卖家发布的商品
        }
//        String attr = _allCols[AuctionAuctions.FEATURES];
//        String biz = TaobaoPath.getValue(attr, "biz", null);
//        if ("alimall".equals(biz)) {
//            output.collect(Utils.mergeKey("total", pre + "_alimall"), ONE);  //无名良品卖家发布的商品
//        }

    }

    protected void setup(String[] args) {
        this.taskNum = 100;
    }


    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),
                new MallItemCenter(), args);
        System.exit(res);
    }


}
