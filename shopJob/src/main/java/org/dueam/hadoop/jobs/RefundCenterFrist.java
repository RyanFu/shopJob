package org.dueam.hadoop.jobs;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.LongSumReducer;
import org.apache.hadoop.util.ToolRunner;
import org.dueam.hadoop.common.HadoopTable;
import org.dueam.hadoop.common.MapReduce;
import org.dueam.hadoop.common.tables.TcRefundTrade;
import org.dueam.hadoop.common.util.DateStringUtils;
import org.dueam.hadoop.common.util.Utils;

import java.io.IOException;
import java.util.Iterator;

/**
 * 交易中心报表
 * User: windonly
 * Date: 10-12-16 下午5:44
 */
public class RefundCenterFrist extends MapReduce<Text, LongWritable> {


    @Override
    protected void setup(String[] args) {
        this.output = "/group/tbdev/xiaodu/suoni/refund_center_frist/" + args[0];
        this.inputFormat = HadoopTable.refund(args[0]).getInputFormat();
        this.input = HadoopTable.refund(args[0]).getInputPath();
    }

    @Override
    protected void beforeRun(String[] args, JobConf config) {
        config.setCombinerClass(LongSumReducer.class);
        config.setReducerClass(LongSumReducer.class);
    }

    @Override
    @Deprecated  //直接采用LongSumReducer了
    public void reduce(Text text, Iterator<LongWritable> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

    }

    @Override
    public void map(Object key, Text value, OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException {
        String[] _allCols = StringUtils.splitPreserveAllTokens(value.toString(), TAB);
        if (_allCols.length < 41) {
            return;
        }

        String queryDate = DateStringUtils.format(inputArgs[0]);
        String gmtCreated = _allCols[TcRefundTrade.gmt_create];
        String gmtModified = _allCols[TcRefundTrade.gmt_modified];

        String attributes = _allCols[TcRefundTrade.attributes];
        if(!"1".equals(Utils.getValue(attributes,"V3Process","null")))  {
            return;
        }

        //今天创建的订单相关统计
        if (Utils.isSameDay(queryDate, gmtCreated)) {
            //今天创建的退款单数
            output.collect(Utils.mergeKey("total", "today_created"), ONE);
            //需要退款的金额
            output.collect(Utils.mergeKey("total", "today_return_fee"), new LongWritable(NumberUtils.toLong(_allCols[TcRefundTrade.return_fee])));
            //需要退款订单总金额
            output.collect(Utils.mergeKey("total", "today_total_fee"), new LongWritable(NumberUtils.toLong(_allCols[TcRefundTrade.total_fee])));
        }
        if (Utils.isSameDay(queryDate, gmtModified)) {
            output.collect(Utils.mergeKey("total", "today_modified"), ONE);
            if ("4".equals(_allCols[TcRefundTrade.refund_status]) || "5".equals(_allCols[TcRefundTrade.refund_status])) {
                long second = DateStringUtils.second(gmtCreated, gmtModified);
                output.collect(Utils.mergeKey("total", "cast_time_sum"), new LongWritable(second));
                output.collect(Utils.mergeKey("total", "cast_time_num"), ONE);
                //需要退货的订单时间
                if("1".equals(_allCols[TcRefundTrade.need_return_goods])){
                    output.collect(Utils.mergeKey("total", "cast_time_goods_sum"), new LongWritable(second));
                    output.collect(Utils.mergeKey("total", "cast_time_goods_num"), ONE);
                    output.collect(Utils.mergeKey("cast_time_goods", TcRefundTrade.refundCastTimeArea.getArea(second)), ONE);
                } else {
                    output.collect(Utils.mergeKey("total", "cast_time_no_goods_sum"), new LongWritable(second));
                    output.collect(Utils.mergeKey("total", "cast_time_no_goods_num"), ONE);
                    output.collect(Utils.mergeKey("cast_time_no_goods", TcRefundTrade.refundCastTimeArea.getArea(second)), ONE);
                }

                output.collect(Utils.mergeKey("cast_time", TcRefundTrade.refundCastTimeArea.getArea(second)), ONE);
            }
        }
        output.collect(Utils.mergeKey("total", "all"), ONE);


        //group by refund_status
        //全网 refund_status
        output.collect(Utils.mergeKey("group_refund_status", _allCols[TcRefundTrade.refund_status]), ONE);

        //group by cs_status
        output.collect(Utils.mergeKey("group_cs_status", _allCols[TcRefundTrade.cs_status]), ONE);

        String lastWeek = DateStringUtils.add(queryDate + " 00:00:00", -7, DateStringUtils.DEFAULT_DATETIME_STYLE);

        if (gmtCreated.compareTo(lastWeek + " 00:00:00") >= 0) {
            //group by refund_status
            //全网 refund_status
            output.collect(Utils.mergeKey("group_last_week_refund_status", _allCols[TcRefundTrade.refund_status]), ONE);

            //group by cs_status
            output.collect(Utils.mergeKey("group_last_week_cs_status", _allCols[TcRefundTrade.cs_status]), ONE);
        }


    }


    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),
                new RefundCenterFrist(), args);
        System.exit(res);
    }


}
