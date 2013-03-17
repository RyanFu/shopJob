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
import org.dueam.hadoop.common.tables.BmwShops;
import org.dueam.hadoop.common.util.DateStringUtils;
import org.dueam.hadoop.common.util.Utils;

import javax.rmi.CORBA.Util;
import java.io.IOException;

/**
 * 追踪店铺增长情况
 */
public class ShopCenter extends SimpleMapReduce {
    @Override
    protected String[] getInputPath(String[] args) {
        String[] input = HadoopTable.shop(args[0]).getInputPath();
        System.out.println("input path => " + Utils.asString(input));
        return input;
    }

    @Override
    protected Class getInputFormat() {
        return HadoopTable.shop(null).getInputFormat();
    }

    @Override
    protected String getOutputPath(String[] args) {
        return "/group/tbdev/xiaodu/suoni/shop_center/" + args[0];

    }


    @Override
    protected void beforeRun(String[] args, JobConf config) {
        config.setCombinerClass(LongSumReducer.class);
    }

    @Override
    protected void doWork(String line, OutputCollector<Text, LongWritable> output) throws IOException {
        String[] _allCols = StringUtils.splitPreserveAllTokens(line, TAB);
        if (_allCols.length < 22) {
            return;
        }

        String queryDate = DateStringUtils.format(inputArgs[0]);

        output.collect(Utils.mergeKey("total", "all"), ONE);  //所有店铺数
        if (Utils.isSameDay(queryDate, _allCols[BmwShops.starts])) {
            output.collect(Utils.mergeKey("total", "today"), ONE); //今天开的店铺
        }
        //店铺状态
        output.collect(Utils.mergeKey("approve_status", _allCols[BmwShops.approve_status]), ONE);

        //统计正常的店铺

        if ("0".equals(_allCols[BmwShops.approve_status])) {
            long promotedStatus = NumberUtils.toLong(_allCols[BmwShops.promoted_status]);
            for (long status : Utils.toBitArray(promotedStatus)) {
                output.collect(Utils.mergeKey("promoted_status", String.valueOf(status)), ONE);
            }
            long produceCount = NumberUtils.toLong(_allCols[BmwShops.products_count]);
            if (produceCount >=0) {
                output.collect(Utils.mergeKey("product_count", BmwShops.getArea(produceCount)), ONE);
            }
            //店铺类型
            output.collect(Utils.mergeKey("site", _allCols[BmwShops.siteid]), ONE);

            if (Utils.isSameDay(queryDate, _allCols[BmwShops.starts])) {
                //店铺类型
                output.collect(Utils.mergeKey("today_site", _allCols[BmwShops.siteid]), ONE); //今天开的店铺分布
            }
        }


    }


    protected void setup(String[] args) {
        this.taskNum = 100;
    }


    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),
                new ShopCenter(), args);
        System.exit(res);
    }


}
