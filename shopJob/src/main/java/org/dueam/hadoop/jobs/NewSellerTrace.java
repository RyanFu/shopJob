package org.dueam.hadoop.jobs;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.LongSumReducer;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.dueam.hadoop.common.Area;
import org.dueam.hadoop.common.tables.BmwUsers;
import org.dueam.hadoop.common.tables.FeedReceive;
import org.dueam.hadoop.common.util.DateStringUtils;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.hadoop.utils.uic.UicUtils;

import java.io.IOException;

/**
 * 新卖家开店一周之后相关信息追踪
 * User: windonly
 * Date: 11-4-7 上午9:36
 */
public class NewSellerTrace extends Configured implements Tool {
    public int run(String[] args) throws Exception {
        if (args.length < 1) {
            args = new String[]{DateStringUtils.now()};
            System.out.println("ERROR: Please Enter Date , eg. 20101010 ! now use default => " + DateStringUtils.now());

        }

        if (true) {
            JobConf config = new JobConf(getConf(), getClass());
            config.set("user.args", Utils.asString(args));

            config.setJobName(getClass().getSimpleName() + "-" + System.currentTimeMillis());
            config.setNumReduceTasks(5);
            config.setReducerClass(LongSumReducer.class);
            config.setCombinerClass(LongSumReducer.class);
            config.setInputFormat(TextInputFormat.class);
            config.setMapOutputKeyClass(Text.class);
            config.setMapOutputValueClass(LongWritable.class);


            //add input paths
            MultipleInputs.addInputPath(config, new Path("/group/tbptd-dev/xiaodu/hive/today_open_shop_seller_info/pt=" + args[0]), TextInputFormat.class,BmwUsersMapper.class);
            MultipleInputs.addInputPath(config, new Path("/group/tbptd-dev/xiaodu/hive/today_open_shop_seller_feed/pt=" + args[0]), TextInputFormat.class,FeedReceiveMapper.class);

            config.setOutputKeyClass(Text.class);
            config.setOutputValueClass(LongWritable.class);

            //if output path exists then return
            FileSystem fs = FileSystem.get(config);
            Path outputPath = new Path("/group/tbdev/xiaodu/suoni/new_seller_trace/"+args[0]);
            FileOutputFormat.setOutputPath(config, outputPath);

            if (!fs.exists(outputPath)) {
                JobClient.runJob(config);
            } else {
                System.out.println("You has finished this job today ! " + outputPath);
            }

        }


        return JobClient.SUCCESS;
    }

    public final static char TAB = 0x09;

    public final static LongWritable ONE = new LongWritable(1);

    /**
     * 一周后用户表的相关信息统计：用户的信用等级分布
     */
    public static class BmwUsersMapper implements Mapper<Object,Text,Text,LongWritable>{

        public void map(Object key, Text value, OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException {
            String[] _allCols = StringUtils.splitPreserveAllTokens(value.toString(),TAB)  ;
            if(_allCols.length < 60)return;
            long sellerRate = (long)NumberUtils.toDouble(_allCols[BmwUsers.RATE_SUM]);
            output.collect(Utils.mergeKey("rate", UicUtils.getRate(sellerRate)),ONE);
            output.collect(Utils.mergeKey("total", "all"),ONE);
        }

        public void close() throws IOException {
        }

        public void configure(JobConf job) {
        }
    }
    public static Area ITEM_PRICE = Area.newArea(new long[]{0,1,2,5,10,20,50,100,200,500});
    public static class FeedReceiveMapper implements Mapper<Object,Text,Text,LongWritable>{

        public void map(Object key, Text value, OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException {
            String[] _allCols = StringUtils.splitPreserveAllTokens(value.toString(),TAB)  ;
            if(_allCols.length < 31 )return;
            String seller = _allCols[FeedReceive.rated_uid];
            String buyer = _allCols[FeedReceive.rater_uid];
            output.collect(Utils.mergeKey("seller", seller),ONE);
            output.collect(Utils.mergeKey("seller_and_buyer", seller,buyer),ONE);
            output.collect(Utils.mergeKey("total", "rate"),ONE);
            output.collect(Utils.mergeKey("total","rate_"+_allCols[FeedReceive.rate]),ONE);

            double price = NumberUtils.toDouble(_allCols[FeedReceive.auction_price]) ;
            //发生评价商品的价格区间
            output.collect(Utils.mergeKey("item_price", ITEM_PRICE.getArea(price)),ONE);
        }

        public void close() throws IOException {
        }

        public void configure(JobConf job) {
        }
    }





    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new NewSellerTrace(),
                args);
        System.exit(res);
    }
}
