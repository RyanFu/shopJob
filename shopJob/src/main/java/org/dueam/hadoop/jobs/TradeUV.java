package org.dueam.hadoop.jobs;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.LongSumReducer;
import org.apache.hadoop.util.ToolRunner;
import org.dueam.hadoop.common.HadoopTable;
import org.dueam.hadoop.common.SimpleMapReduce;
import org.dueam.hadoop.common.tables.TcBizOrder;
import org.dueam.hadoop.common.util.DateStringUtils;
import org.dueam.hadoop.common.util.Utils;

import java.io.IOException;

/**
 * 交易UV报表
 * User: windonly
 * Date: 10-12-16 下午5:44
 */
public class TradeUV extends SimpleMapReduce {

    public int run(String[] args) throws Exception {
        if (args.length < 1) {
            args = new String[]{DateStringUtils.now()};
            System.out.println("ERROR: Please Enter Date , eg. 20101010 ! now use default => " + DateStringUtils.now());
        }
        setup(args);

        if (true) {
            JobConf config = new JobConf(getConf(), getClass());
            config.set("user.args", Utils.asString(args));

            config.setJobName(getName() + " step 1 -" + System.currentTimeMillis());
            config.setNumReduceTasks(10);
            config.setMapperClass(getClass());
            config.setReducerClass(LongSumReducer.class);
            config.setInputFormat(getInputFormat());
            config.setMapOutputKeyClass(Text.class);
            config.setMapOutputValueClass(LongWritable.class);
            config.setCombinerClass(LongSumReducer.class);
            //add input paths
            for (String path : getInputPath(args)) {
                if (TextInputFormat.class.equals(getInputFormat())) {
                    TextInputFormat.addInputPath(config, new Path(path));
                } else if (SequenceFileInputFormat.class.equals(getInputFormat())) {
                    SequenceFileInputFormat.addInputPath(config, new Path(path));
                }
            }

            config.setOutputKeyClass(Text.class);
            config.setOutputValueClass(Text.class);

            //if output path exists then return
            FileSystem fs = FileSystem.get(config);
            Path outputPath = new Path(getOutputPath(args));
            FileOutputFormat.setOutputPath(config, outputPath);

            beforeRun(args, config);

            if (!fs.exists(outputPath)) {
                JobClient.runJob(config);
            } else {
                System.out.println("You has finished this job today ! " + outputPath);
            }
        }


        if (true) {
            JobConf config = new JobConf(getConf(), getClass());
            config.set("user.args", Utils.asString(args));

            config.setJobName(getName() + " step 2 -" + System.currentTimeMillis());
            config.setNumReduceTasks(1);
            config.setMapperClass(StepTwo.class);
            config.setReducerClass(LongSumReducer.class);
            config.setInputFormat(TextInputFormat.class);
            config.setMapOutputKeyClass(Text.class);
            config.setMapOutputValueClass(LongWritable.class);
            config.setCombinerClass(LongSumReducer.class);

            TextInputFormat.addInputPath(config, new Path(getOutputPath(args)));

            config.setOutputKeyClass(Text.class);
            config.setOutputValueClass(Text.class);

            //if output path exists then return
            FileSystem fs = FileSystem.get(config);
            Path outputPath = new Path("/group/tbdev/xiaodu/suoni/trade_uv/" + args[0]);
            FileOutputFormat.setOutputPath(config, outputPath);

            beforeRun(args, config);

            if (!fs.exists(outputPath)) {
                JobClient.runJob(config);
            } else {
                System.out.println("You has finished this job today ! " + outputPath);
            }
        }

        return JobClient.SUCCESS;
    }

    public static class StepTwo extends MapReduceBase implements Mapper<Object, Text, Text, LongWritable> {

        public void map(Object key, Text value, OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException {
            String[] _allCols = StringUtils.splitPreserveAllTokens(value.toString(), TAB);
            if (_allCols.length < 3) {
                return;
            }
            if (_allCols[0].endsWith("seller") || _allCols[0].endsWith("buyer")) {
                output.collect(Utils.mergeKey(_allCols[0] + "_uv"), ONE);
            } else {
                output.collect(Utils.mergeKey(_allCols[0] + "_" + _allCols[1]), new LongWritable(NumberUtils.toLong(_allCols[2])));
            }
        }
    }

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
        return "/group/tbdev/xiaodu/suoni/trade_uv_source/" + args[0];

    }

    @Override
    protected void beforeRun(String[] args, JobConf config) {

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

        if ("1".equals(Utils.getValue(_allCols[TcBizOrder.ATTRIBUTES], "cart", "0"))) {
            if (Utils.isSameDay(gmtCreated, queryDate)) {
                long fee = TcBizOrder.getTotalFee(_allCols);        //大订单过滤
                boolean bigOrder = false;
                if (fee > 100000 * 100) {
                    String catId = Utils.getValue(_allCols[TcBizOrder.ATTRIBUTES], "realRootCat", "NULL");
                    if ("26".equals(catId)) {
                        if (fee > 500000 * 100) bigOrder = true;
                    } else {
                        bigOrder = true;
                    }
                }
                if (!bigOrder) {
                    if (TcBizOrder.isMain(_allCols)) {
                        output.collect(Utils.mergeKey("cart_gmv_trade", "main_pv"), ONE);
                        output.collect(Utils.mergeKey("cart_gmv_trade", "main_sum"), new LongWritable(TcBizOrder.getTotalFee(_allCols)));
                    }
//                if (TcBizOrder.isDetail(_allCols)) {
//                    output.collect(Utils.mergeKey("cart_gmv_trade", "detail_pv"), ONE);
//                }
                    if (TcBizOrder.isMain(_allCols)) {
                        output.collect(Utils.mergeKey("cart_gmv_buyer", _allCols[TcBizOrder.BUYER_ID]), new LongWritable(TcBizOrder.getTotalFee(_allCols)));
                    }

                    if (TcBizOrder.isMain(_allCols)) {
                        output.collect(Utils.mergeKey("cart_gmv_seller", _allCols[TcBizOrder.SELLER_ID]), new LongWritable(TcBizOrder.getTotalFee(_allCols)));
                    }
                }

            }


            //alipay
            if (Utils.isSameDay(payTime, queryDate) && TcBizOrder.isPaied(_allCols)) {
                if (TcBizOrder.isMain(_allCols)) {
                    output.collect(Utils.mergeKey("cart_alipay_trade", "main_pv"), ONE);
                    output.collect(Utils.mergeKey("cart_alipay_trade", "main_sum"), new LongWritable(TcBizOrder.getTotalFee(_allCols)));
                }
//                if (TcBizOrder.isDetail(_allCols)) {
//                    output.collect(Utils.mergeKey("cart_alipay_trade", "detail_pv"), ONE);
//                }
                if (TcBizOrder.isMain(_allCols)) {
                    output.collect(Utils.mergeKey("cart_alipay_buyer", _allCols[TcBizOrder.BUYER_ID]), new LongWritable(TcBizOrder.getTotalFee(_allCols)));
                }

                if (TcBizOrder.isMain(_allCols)) {
                    output.collect(Utils.mergeKey("cart_alipay_seller", _allCols[TcBizOrder.SELLER_ID]), new LongWritable(TcBizOrder.getTotalFee(_allCols)));
                }
            }
        }
        //gmv
        if (Utils.isSameDay(gmtCreated, queryDate)) {
            if (Utils.isSameDay(gmtCreated, queryDate)) {
//                if (TcBizOrder.isDetail(_allCols)) {
//                    output.collect(Utils.mergeKey("gmv_trade", "detail_pv"), ONE);
//                }
                long fee = TcBizOrder.getTotalFee(_allCols);        //大订单过滤
                boolean bigOrder = false;
                if (fee > 100000 * 100) {
                    String catId = Utils.getValue(_allCols[TcBizOrder.ATTRIBUTES], "realRootCat", "NULL");
                    if ("26".equals(catId)) {
                        if (fee > 500000 * 100) bigOrder = true;
                    } else {
                        bigOrder = true;
                    }
                }
                if (!bigOrder) {
                    if (TcBizOrder.isMain(_allCols)) {
                        output.collect(Utils.mergeKey("gmv_trade", "main_pv"), ONE);
                        output.collect(Utils.mergeKey("gmv_trade", "main_sum"), new LongWritable(TcBizOrder.getTotalFee(_allCols)));
                    }

                    if (TcBizOrder.isMain(_allCols)) {
                        output.collect(Utils.mergeKey("gmv_buyer", _allCols[TcBizOrder.BUYER_ID]), new LongWritable(TcBizOrder.getTotalFee(_allCols)));
                    }

                    if (TcBizOrder.isMain(_allCols)) {
                        output.collect(Utils.mergeKey("gmv_seller", _allCols[TcBizOrder.SELLER_ID]), new LongWritable(TcBizOrder.getTotalFee(_allCols)));
                    }
                }
            }
        }


        //alipay
        if (Utils.isSameDay(payTime, queryDate) && TcBizOrder.isPaied(_allCols)) {
            if (TcBizOrder.isMain(_allCols)) {
                output.collect(Utils.mergeKey("alipay_trade", "main_pv"), ONE);
                output.collect(Utils.mergeKey("alipay_trade", "main_sum"), new LongWritable(TcBizOrder.getTotalFee(_allCols)));
            }
            if (TcBizOrder.isDetail(_allCols)) {
                output.collect(Utils.mergeKey("alipay_trade", "detail_pv"), ONE);
            }
            if (TcBizOrder.isMain(_allCols)) {
                output.collect(Utils.mergeKey("alipay_buyer", _allCols[TcBizOrder.BUYER_ID]), new LongWritable(TcBizOrder.getTotalFee(_allCols)));
            }

            if (TcBizOrder.isMain(_allCols)) {
                output.collect(Utils.mergeKey("alipay_seller", _allCols[TcBizOrder.SELLER_ID]), new LongWritable(TcBizOrder.getTotalFee(_allCols)));
            }
        }


    }


    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),
                new TradeUV(), args);
        System.exit(res);
    }


}
