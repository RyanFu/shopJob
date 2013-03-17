package org.dueam.hadoop.jobs;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.util.ToolRunner;
import org.dueam.hadoop.common.Area;
import org.dueam.hadoop.common.HadoopTable;
import org.dueam.hadoop.common.MapReduce;
import org.dueam.hadoop.common.tables.TcBizOrder;
import org.dueam.hadoop.common.util.DateStringUtils;
import org.dueam.hadoop.common.util.Utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * 用户交易监控报表
 * User: windonly
 * Date: 10-12-16 下午5:44
 */
public class UserTradeMonitorSecond extends MapReduce<LongWritable, Text> {
    @Override
    protected void setup(String[] args) {
        this.taskNum = 20;
        this.input = HadoopTable.orderDelta(args[0]).getInputPath();
        this.inputFormat = HadoopTable.orderDelta(null).getInputFormat();
        this.output = "/group/tbbp/suoni/user_trade_monitor_second/" + args[0];
    }

    public int run(String[] args) throws Exception {
        if (args.length < 1) {
            args = new String[]{DateStringUtils.now()};
            System.out.println("ERROR: Please Enter Date , eg. 20101010 ! now use default => " + DateStringUtils.now());
        }
        setup(args);

        JobConf config = new JobConf(getConf(), getClass());
        config.set("user.args", Utils.asString(args));

        config.setJobName(getName() + "-" + System.currentTimeMillis());
        config.setNumReduceTasks(taskNum);
        config.setReducerClass(getClass());
        config.setInputFormat(inputFormat);
        config.setMapOutputKeyClass(LongWritable.class);
        config.setMapOutputValueClass(Text.class);

        MultipleInputs.addInputPath(config, new Path("/group/tbbp/suoni/user_trade_monitor/" + args[0]), TextInputFormat.class, StatTradeMapper.class);
        for (String path : input) {
            if (TextInputFormat.class.equals(inputFormat)) {
                MultipleInputs.addInputPath(config, new Path(path), TextInputFormat.class, getClass());
            } else if (SequenceFileInputFormat.class.equals(inputFormat)) {
                MultipleInputs.addInputPath(config, new Path(path), SequenceFileInputFormat.class, getClass());
            }
        }


        config.setOutputKeyClass(Text.class);
        config.setOutputValueClass(Text.class);

        //if output path exists then return
        FileSystem fs = FileSystem.get(config);
        Path outputPath = new Path(output);
        FileOutputFormat.setOutputPath(config, outputPath);

        beforeRun(args, config);

        if (!fs.exists(outputPath)) {
            JobClient.runJob(config);
        } else {
            System.out.println("You has finished this job today ! " + outputPath);
        }

        return JobClient.SUCCESS;
    }

    public static class StatTradeMapper implements Mapper<Object, Text, LongWritable, Text> {

        public void map(Object key, Text value, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
            String[] _allCols = StringUtils.splitPreserveAllTokens(value.toString(), TAB);
            long buyerId = NumberUtils.toLong(_allCols[0]);
            if (buyerId > 0) {
                output.collect(new LongWritable(buyerId), Utils.mergeKey("SECOND", _allCols[1], _allCols[2], _allCols[3]));
            }
        }

        public void close() throws IOException {
        }

        public void configure(JobConf job) {
        }
    }

    @Override
    protected void beforeRun(String[] args, JobConf config) {
        config.setMapOutputValueClass(Text.class);
    }

    @Override
    public void reduce(LongWritable key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        List<String[]> temp = new ArrayList<String[]>();
        String[] statCode = null;
        while (values.hasNext()) {
            String[] array = StringUtils.splitPreserveAllTokens(values.next().toString(), TAB);
            if ("SECOND".equals(array[0])) {
                statCode = array;
            } else {
               // output.collect(new Text(String.valueOf(key.get())),Utils.mergeKey(array));
                temp.add(array);
            }

        }
        if (statCode != null) {
            for (String[] array : temp) {
                //
                doStat(key.get(), statCode, array, output);
            }
        }

        /*
        CounterMap cat = CounterMap.newCounter("cat");
        CounterMap price = CounterMap.newCounter("price");
        CounterMap payTime = CounterMap.newCounter("PayTime");

        while (values.hasNext()) {
            String[] array = StringUtils.splitPreserveAllTokens(values.next().toString(), TAB);
            cat.add(array[0], 1);
            price.add(array[1], 1);
            payTime.add(array[2], 1);
        }
        output.collect(key, Utils.mergeKey(cat.toString(false, ':', ';'), price.toString(false, ':', ';'), payTime.toString(false, ':', ';')));
        */
    }

    private void doStat(long buyerId, String[] statCode, String[] inputArray, OutputCollector<Text, Text> output) throws IOException {
        String catStat = statCode[1];
        String priceStat = statCode[2];
        String payTimeStat = statCode[3];
        String cat = inputArray[0];
        String price = inputArray[1];
        String payTime = inputArray[2];
        if (sum(catStat) > 30) {
            int code = stat(catStat, cat, 100) + stat(payTimeStat, payTime, 100);
            //output.collect(Utils.mergeKey(String.valueOf(buyerId),String.valueOf(code)), Utils.mergeKey(inputArray, statCode));
            if (code < 10) {
                output.collect(new Text(String.valueOf(buyerId)), Utils.mergeKey(inputArray, statCode));
            }
        }

    }

    public int stat(String statCode, String stat, int code) {
        statCode = ';' + statCode;
        if (StringUtils.indexOf(statCode, ';' + stat + ':') < 0) {
            return -1;
        }
        int v = -1;
        int sum = 0;
        for (String eqString : StringUtils.split(statCode, ';')) {
            String[] array = StringUtils.split(eqString, ':');
            if (array.length < 2) continue;
            if (stat.equals(array[0])) {
                v = NumberUtils.toInt(array[1]);
            }
            sum += NumberUtils.toInt(array[1]);
        }

        if (v > 0) {
            return code * v / sum;
        }
        return -1;
    }

    public int sum(String statCode) {
        int sum = 0;
        for (String eqString : StringUtils.split(statCode, ';')) {
            String[] array = StringUtils.split(eqString, ':');
            sum += NumberUtils.toInt(array[1]);
        }
        return sum;
    }


    @Override
    public void map(Object key, Text value, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
        String[] _allCols = StringUtils.splitPreserveAllTokens(value.toString(), TAB);
        if (_allCols.length < 47) {
            return;
        }
        String queryDate = DateStringUtils.format(inputArgs[0]);
        String userPayTime = _allCols[TcBizOrder.PAY_TIME];
        if (TcBizOrder.isPaied(_allCols) && TcBizOrder.isDetail(_allCols) && Utils.isSameDay(queryDate, userPayTime)) {
            if (!("100".equals(_allCols[TcBizOrder.BIZ_TYPE]) || "300".equals(_allCols[TcBizOrder.BIZ_TYPE]))) {
                return;
            }
            long buyerId = NumberUtils.toLong(_allCols[TcBizOrder.BUYER_ID]);
            String payTime = StringUtils.substring(_allCols[TcBizOrder.PAY_TIME], 11, 13);
            payTime = String.valueOf(NumberUtils.toInt(payTime) - NumberUtils.toInt(payTime) % 3);
            String bizOrderId = _allCols[TcBizOrder.BIZ_ORDER_ID];
            String buyerNick = _allCols[TcBizOrder.BUYER_NICK];
            String priceArea = alipayTradeArea.getArea(NumberUtils.toLong(_allCols[TcBizOrder.AUCTION_PRICE]));
            String rootCatId = Utils.getValue(_allCols[TcBizOrder.ATTRIBUTES], "realRootCat", "NULL");
            if (buyerId > 0) {
                output.collect(new LongWritable(buyerId), Utils.mergeKey(rootCatId, priceArea, payTime, bizOrderId, buyerNick,_allCols[TcBizOrder.BIZ_TYPE]));
            }
        }
    }


    static Area alipayTradeArea = Area.newArea(Area.MoneyCallback, 0, 1 * 100, 10 * 100, 50 * 100, 100 * 100, 200 * 100, 300 * 100, 500 * 100, 10000 * 100, 100000 * 100);


    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),
                new UserTradeMonitorSecond(), args);
        if (res == JobClient.SUCCESS) {

        }
        System.exit(res);
    }


}
