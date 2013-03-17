package org.dueam.hadoop.jobs;

import org.apache.commons.lang.ArrayUtils;
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
import java.util.Arrays;

/**
 * DPC 指标汇总
 */
public class DpcCenter extends SimpleMapReduce {
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
        return "/group/tbdev/xiaodu/suoni/dpc_center/" + args[0];

    }


    @Override
    protected void beforeRun(String[] args, JobConf config) {
        config.setCombinerClass(LongSumReducer.class);
    }

    final static long DPC_OPTIONS = 2L << 34;

    @Override
    protected void doWork(String line, OutputCollector<Text, LongWritable> output) throws IOException {
        String[] _allCols = StringUtils.splitPreserveAllTokens(line, CTRL_A);
        if (_allCols.length < 51) {
            return;
        }

        String queryDate = DateStringUtils.format(inputArgs[0]);
        String gmtCreated = _allCols[AuctionAuctions.OLD_STARTS];
        String status = _allCols[AuctionAuctions.AUCTION_STATUS];
        String categoryId = _allCols[AuctionAuctions.CATEGORY];
        String gmtModified = _allCols[AuctionAuctions.GMT_MODIFIED];
        String lastModified = _allCols[AuctionAuctions.LAST_MODIFIED];

        long options = NumberUtils.toLong(_allCols[AuctionAuctions.OPTIONS]);
        String[] tags = StringUtils.split(Utils.getValue(_allCols[AuctionAuctions.FEATURES], "tags", ""), ',');

        if ((options & DPC_OPTIONS) > 0) {
            output.collect(Utils.mergeKey("dpc_total", "online"), ONE);
            if (ArrayUtils.contains(tags, "66")) {
                output.collect(Utils.mergeKey("dpc_total", "brand"), ONE);
            }
            if (ArrayUtils.contains(tags, "450")) {
                output.collect(Utils.mergeKey("dpc_total", "jingxiao"), ONE);
            } else {
                output.collect(Utils.mergeKey("dpc_total", "daixiao"), ONE);
            }
            output.collect(Utils.mergeKey("dpc_total", "all"), ONE);
            output.collect(Utils.mergeKey("db_status", status), ONE);  //整个DB里的商品状态分布
            //类目发布
            output.collect(Utils.mergeKey("db_category", categoryId), ONE);
        } else if (ArrayUtils.contains(tags, "2")) {
            output.collect(Utils.mergeKey("dpc_total", "line"), ONE);
            output.collect(Utils.mergeKey("dpc_total", "all"), ONE);
            output.collect(Utils.mergeKey("db_status", status), ONE);  //整个DB里的商品状态分布
            //类目发布
            output.collect(Utils.mergeKey("db_category", categoryId), ONE);
        }

        if (Utils.isSameDay(queryDate, gmtCreated)) {
            if ((options & DPC_OPTIONS) > 0) {
                output.collect(Utils.mergeKey("dpc_today", "online"), ONE);
                if (ArrayUtils.contains(tags, "66")) {
                    output.collect(Utils.mergeKey("dpc_today", "brand"), ONE);
                }
                if (ArrayUtils.contains(tags, "450")) {
                    output.collect(Utils.mergeKey("dpc_today", "jingxiao"), ONE);
                } else {
                    output.collect(Utils.mergeKey("dpc_today", "daixiao"), ONE);
                }
                output.collect(Utils.mergeKey("dpc_today", "all"), ONE);
                output.collect(Utils.mergeKey("today_status", status), ONE);  //今天发布的商品状态分布
            } else if (ArrayUtils.contains(tags, "2")) {
                output.collect(Utils.mergeKey("dpc_today", "line"), ONE);
                output.collect(Utils.mergeKey("dpc_today", "all"), ONE);
                output.collect(Utils.mergeKey("today_status", status), ONE);  //今天发布的商品状态分布
            }

        }

        if (Utils.isSameDay(queryDate, gmtModified)) {
            if ((options & DPC_OPTIONS) > 0 || ArrayUtils.contains(tags, "2")) {
                output.collect(Utils.mergeKey("dpc_today", "today_modified"), ONE);
            }
        }

        if (Utils.isSameDay(queryDate, lastModified)) {
            if ((options & DPC_OPTIONS) > 0 || ArrayUtils.contains(tags, "2")) {
                output.collect(Utils.mergeKey("dpc_today", "today_last_modified"), ONE);
            }
        }

    }

    protected void setup(String[] args) {
        this.taskNum = 30;
    }


    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),
                new DpcCenter(), args);
        System.exit(res);
    }


}
