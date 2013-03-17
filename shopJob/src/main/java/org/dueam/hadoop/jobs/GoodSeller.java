package org.dueam.hadoop.jobs;

import org.apache.commons.lang.ArrayUtils;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.dueam.hadoop.common.Area;
import org.dueam.hadoop.common.HadoopTable;
import org.dueam.hadoop.common.Table;
import org.dueam.hadoop.common.tables.TcBizOrder;
import org.dueam.hadoop.common.util.DateStringUtils;
import org.dueam.hadoop.common.util.Utils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

/**
 * User: windonly
 * Date: 11-7-25 下午1:35
 */
public class GoodSeller extends Configured implements Tool {
    public int run(String[] args) throws Exception {
        if (args.length < 1) {
            args = new String[]{DateStringUtils.now()};
            System.out.println("ERROR: Please Enter Date , eg. 20101010 ! now use default => " + DateStringUtils.now());
        }

        System.out.println(Utils.asString(args));
        //step 1 : group by seller_id,buyer_id,buy_date,main_order_count
        String type = "all";
        if (args.length > 1) {
            type = args[1];
        }
        String outputStepOne = "/group/tbbp/suoni/good_seller/" + args[0] + "/" + type + "/step_one";
        String outputStepTwo = "/group/tbbp/suoni/good_seller/" + args[0] + "/" + type + "/step_two";
        String outputStepThree = "/group/tbbp/suoni/good_seller/" + args[0] + "/" + type + "/step_three";
        String outputStepFour = "/group/tbbp/suoni/good_seller/" + args[0] + "/" + type + "/step_four";

        if (true) {
            Table table = HadoopTable.order(args[0], "200");
            JobConf config = new JobConf(getConf(), getClass());
            config.set("user.args", Utils.asString(args));
            if (args.length > 1) {
                config.set("catIds", type);
            }

            config.setJobName("GoodSeller - step 1 -" + System.currentTimeMillis());
            config.setNumReduceTasks(100);
            config.setMapperClass(StepOne.class);
            config.setReducerClass(LongSumReducer.class);
            config.setInputFormat(table.getInputFormat());
            config.setMapOutputKeyClass(Text.class);
            config.setMapOutputValueClass(LongWritable.class);
            config.setCombinerClass(LongSumReducer.class);

            //add input paths
            for (String path : table.getInputPath()) {
                if (TextInputFormat.class.equals(table.getInputFormat())) {
                    TextInputFormat.addInputPath(config, new Path(path));
                } else if (SequenceFileInputFormat.class.equals(table.getInputFormat())) {
                    SequenceFileInputFormat.addInputPath(config, new Path(path));
                }
            }

            config.setOutputKeyClass(Text.class);
            config.setOutputValueClass(Text.class);

            //if output path exists then return
            FileSystem fs = FileSystem.get(config);
            Path outputPath = new Path(outputStepOne);
            FileOutputFormat.setOutputPath(config, outputPath);

            if (!fs.exists(outputPath)) {
                JobClient.runJob(config);
            } else {
                System.out.println("You has finished this job today ! " + outputPath);
            }
        }

        //step two
        if (true) {
            JobConf config = new JobConf(getConf(), getClass());
            config.set("user.args", Utils.asString(args));

            config.setJobName("GoodSeller - step 2 -" + System.currentTimeMillis());
            config.setNumReduceTasks(10);
            config.setMapperClass(StepTwo.class);
            config.setReducerClass(StepTwo.class);
            config.setInputFormat(TextInputFormat.class);
            config.setMapOutputKeyClass(Text.class);
            config.setMapOutputValueClass(Text.class);

            TextInputFormat.addInputPath(config, new Path(outputStepOne));

            config.setOutputKeyClass(Text.class);
            config.setOutputValueClass(Text.class);

            //if output path exists then return
            FileSystem fs = FileSystem.get(config);
            Path outputPath = new Path(outputStepTwo);
            FileOutputFormat.setOutputPath(config, outputPath);
            if (!fs.exists(outputPath)) {
                JobClient.runJob(config);
            } else {
                System.out.println("You has finished this job today ! " + outputPath);
            }
        }

        if (true) {
            JobConf config = new JobConf(getConf(), getClass());
            config.set("user.args", Utils.asString(args));

            config.setJobName("GoodSeller - step 3 -" + System.currentTimeMillis());
            config.setNumReduceTasks(10);
            config.setMapperClass(StepThree.class);
            config.setReducerClass(StepThree.class);
            config.setInputFormat(TextInputFormat.class);
            config.setMapOutputKeyClass(Text.class);
            config.setMapOutputValueClass(Text.class);

            TextInputFormat.addInputPath(config, new Path(outputStepTwo));

            config.setOutputKeyClass(Text.class);
            config.setOutputValueClass(Text.class);

            //if output path exists then return
            FileSystem fs = FileSystem.get(config);
            Path outputPath = new Path(outputStepThree);
            FileOutputFormat.setOutputPath(config, outputPath);
            if (!fs.exists(outputPath)) {
                JobClient.runJob(config);
            } else {
                System.out.println("You has finished this job today ! " + outputPath);
            }
        }

        if (true) {
            JobConf config = new JobConf(getConf(), getClass());
            config.set("user.args", Utils.asString(args));

            config.setJobName("GoodSeller - step 3 -" + System.currentTimeMillis());
            config.setNumReduceTasks(10);
            config.setMapperClass(StepFour.class);
            config.setReducerClass(StepFour.class);
            config.setInputFormat(TextInputFormat.class);
            config.setMapOutputKeyClass(Text.class);
            config.setMapOutputValueClass(Text.class);

            TextInputFormat.addInputPath(config, new Path(outputStepThree));

            config.setOutputKeyClass(Text.class);
            config.setOutputValueClass(Text.class);

            //if output path exists then return
            FileSystem fs = FileSystem.get(config);
            Path outputPath = new Path(outputStepFour);
            FileOutputFormat.setOutputPath(config, outputPath);
            if (!fs.exists(outputPath)) {
                JobClient.runJob(config);
            } else {
                System.out.println("You has finished this job today ! " + outputPath);
            }
        }

        return JobClient.SUCCESS;
    }

    private static char TAB = (char) 0x09;
    private static LongWritable ONE = new LongWritable(1);

    /**
     * group by seller,buyer,gmt_create order_count
     */
    public static class StepOne extends MapReduceBase implements Mapper<Object, Text, Text, LongWritable> {
        @Override
        public void configure(JobConf job) {
            if (job.get("catIds") != null) {
                catIds = "," + job.get("catIds") + ",";
            }
        }

        private String catIds;

        public void map(Object key, Text value, OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException {
            String[] _allCols = StringUtils.splitPreserveAllTokens(value.toString(), TAB);
            if (_allCols.length < 47) {
                return;
            }

            //只统计已经付款过的订单
            if (TcBizOrder.isPaied(_allCols) && TcBizOrder.isDetail(_allCols)) {
                String catId = Utils.getValue(_allCols[TcBizOrder.ATTRIBUTES], "realRootCat", "NULL");
                if (null != catIds) {
                    //filter by cat
                    if (StringUtils.indexOf(catIds, catId) < 0) {
                        return;
                    }
                }
                output.collect(Utils.mergeKey(catId, _allCols[TcBizOrder.SELLER_ID], _allCols[TcBizOrder.BUYER_ID], DateStringUtils.getDate(_allCols[TcBizOrder.GMT_CREATE])), ONE);

            }
        }


    }

    /**
     * group by rootCat,seller,buyer dayCount,orderCount
     */
    public static class StepTwo extends MapReduceBase implements Mapper<Object, Text, Text, Text>, Reducer<Text, Text, Text, Text> {

        public void map(Object key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            String[] _allCols = StringUtils.splitPreserveAllTokens(value.toString(), TAB);
            if (_allCols.length < 4) {
                return;
            }

            output.collect(Utils.mergeKey(_allCols[0], _allCols[1], _allCols[2]), Utils.mergeKey("1", _allCols[4]));

        }


        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            long dayCount = 0;
            long orderCount = 0;
            while (values.hasNext()) {
                String[] array = StringUtils.splitPreserveAllTokens(values.next().toString(), TAB);
                dayCount += NumberUtils.toLong(array[0]);
                orderCount += NumberUtils.toLong(array[1]);
            }
            output.collect(key, Utils.mergeKey(String.valueOf(dayCount), String.valueOf(orderCount)));
        }
    }


    public static class StepThree extends MapReduceBase implements Mapper<Object, Text, Text, Text>, Reducer<Text, Text, Text, Text> {

        public void map(Object key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            String[] _allCols = StringUtils.splitPreserveAllTokens(value.toString(), TAB);
            if (_allCols.length < 4) {
                return;
            }
            output.collect(Utils.mergeKey(_allCols[0], _allCols[1]), Utils.mergeKey(_allCols[3], _allCols[4]));

        }


        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            long buyerCount = 0;
            long orderCount = 0;
            Area area = Area.newArea(1, 2, 5, 10, 20, 50, 100);
            while (values.hasNext()) {
                String[] array = StringUtils.splitPreserveAllTokens(values.next().toString(), TAB);
                buyerCount++;
                long dayCount = NumberUtils.toLong(array[0]);
                orderCount += NumberUtils.toLong(array[1]);
                area.count(dayCount);
            }
            StringBuffer sb = new StringBuffer();
            for (String areaKey : area.areaKeys) {
                sb.append(area.get(areaKey)).append(TAB);
            }
            if (sb.length() > 0) {
                sb.deleteCharAt(sb.length() - 1);
            }
            //筛选掉UV <= 100 总订单数 <= 200的卖家
            //if (buyerCount > 100 && orderCount > 200) {
            output.collect(key, Utils.mergeKey(String.valueOf(buyerCount), String.valueOf(orderCount), sb.toString()));
            //}
        }
    }

    //group by rootCat,seller buyerCount,orderCount,countArea
    public static class StepFour extends MapReduceBase implements Mapper<Object, Text, Text, Text>, Reducer<Text, Text, Text, Text> {
        public void map(Object key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            String[] _allCols = StringUtils.splitPreserveAllTokens(value.toString(), TAB);
            if (_allCols.length < 4) {
                return;
            }
            String[] toCopyArray = new String[_allCols.length - 2];
            System.arraycopy(_allCols, 2, toCopyArray, 0, toCopyArray.length);

            output.collect(Utils.mergeKey(_allCols[0]), Utils.mergeKey(toCopyArray));

        }


        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            long buyerCount = 0;
            long orderCount = 0;
            Area area = Area.newArea(1, 2, 5, 10, 20, 50, 100);
            long[] count = new long[area.areaKeys.size()];
            while (values.hasNext()) {
                String[] array = StringUtils.splitPreserveAllTokens(values.next().toString(), TAB);
                buyerCount += NumberUtils.toLong(array[0]);
                orderCount += NumberUtils.toLong(array[1]);
                for (int i = 2; i < array.length; i++) {
                    count[i - 2] += NumberUtils.toLong(array[i]);
                }
            }
            StringBuffer sb = new StringBuffer();
            for (long c : count) {
                sb.append(c).append(TAB);
            }
            if (sb.length() > 0) {
                sb.deleteCharAt(sb.length() - 1);
            }
            //筛选掉UV <= 100 总订单数 <= 200的卖家
            //if (buyerCount > 100 && orderCount > 200) {
            output.collect(key, Utils.mergeKey(String.valueOf(buyerCount), String.valueOf(orderCount), sb.toString()));
            //}
        }
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),
                new GoodSeller(), args);
        System.exit(res);
    }
}
