package org.dueam.hadoop.jobs;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ToolRunner;
import org.dueam.hadoop.common.*;
import org.dueam.hadoop.common.tables.TcBizOrder;
import org.dueam.hadoop.common.util.DateStringUtils;
import org.dueam.hadoop.common.util.Utils;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * 回头客统计
 * User: windonly
 * Date: 10-12-16 下午5:44
 */
public class BuyerComeBack extends MapReduce<Text, Text> {
     public static class SimpleAreaJob extends AreaJob{
         private final Area area = Area.newArea(1,2,3,4,5,10,20,50,100,200);
         @Override
         void mapCallback(Text value, OutputCollector<Text, LongWritable> output) throws IOException {
             String[] _allCols = StringUtils.splitPreserveAllTokens(value.toString(),MapReduce.TAB);
                        String rootCatId = _allCols[0];
                        long count = NumberUtils.toLong(_allCols[3]);
                        output.collect(Utils.mergeKey(area.getArea(count),rootCatId),ONE);
         }
     }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            args = new String[]{DateStringUtils.now()};
            System.out.println("ERROR: Please Enter Date , eg. 20101010 ! now use default => " + DateStringUtils.now());
        }
        int res = ToolRunner.run(new Configuration(),
                new BuyerComeBack(), args);
        String input = HadoopTable.suoniOutput("buyer_come_back", args[0]);
        String output = HadoopTable.suoniOutput("buyer_come_back_area", args[0]);
        res = ToolRunner.run(new Configuration(),
                new SimpleAreaJob() , new String[]{input,output});

        System.exit(res);
    }


    @Override
    protected void setup(String[] args) {
        Table order = HadoopTable.order(args[0], "200");
        this.inputFormat = order.getInputFormat();
        this.input = order.getInputPath();
        this.taskNum = 100;
        this.output = HadoopTable.suoniOutput("buyer_come_back", args[0]);
    }

    @Override
    protected void beforeRun(String[] args, JobConf config) {
        config.setMapOutputValueClass(Text.class);
    }

    @Override
    public void reduce(Text text, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        Set<String> payTimes = new HashSet<String>();
        while (values.hasNext()) {
            payTimes.add(values.next().toString());
        }
        output.collect(text, Utils.mergeKey(String.valueOf(payTimes.size()), StringUtils.join(payTimes, '|')));
    }

    @Override
    public void map(Object key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        String[] _allCols = StringUtils.splitPreserveAllTokens(value.toString(), TAB);
        if (_allCols.length < 47) {
            return;
        }
        String payTime = _allCols[TcBizOrder.PAY_TIME];
        String queryDate = DateStringUtils.format(inputArgs[0]);
        if (TcBizOrder.isDetail(_allCols) && TcBizOrder.isPaied(_allCols)) {
            String rootCatId = Utils.getValue(_allCols[TcBizOrder.ATTRIBUTES], "realRootCat", "NULL");
            Text outputKey = Utils.mergeKey(rootCatId, _allCols[TcBizOrder.SELLER_ID], _allCols[TcBizOrder.BUYER_ID]);
            output.collect(outputKey, new Text(DateStringUtils.getDate(payTime)));
        }
    }
}
