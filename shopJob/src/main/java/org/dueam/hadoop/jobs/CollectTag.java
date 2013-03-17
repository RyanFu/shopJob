package org.dueam.hadoop.jobs;

import org.apache.commons.lang.StringUtils;
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
import org.dueam.hadoop.common.tables.CollectInfo;
import org.dueam.hadoop.common.util.Utils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.dueam.hadoop.common.Functions.str;
import static org.dueam.hadoop.common.util.DateStringUtils.add;
import static org.dueam.hadoop.common.util.DateStringUtils.format;

/**
 * 统计所有女装类目下关键的Top 1000
 * User: windonly
 * Date: 10-12-16 下午5:44
 */
public class CollectTag extends MapReduce<Text, LongWritable> {
    @Override
    protected void beforeRun(String[] args, JobConf config) {
        config.setCombinerClass(LongSumReducer.class);

    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),
                new CollectTag(), args);
        System.exit(res);
    }


    @Override
    protected void setup(String[] args) {
        this.input = HadoopTable.collectInfoAll(args[0]).getInputPath();
        this.inputFormat = HadoopTable.collectInfoAll(args[0]).getInputFormat();
        this.output = "/group/tbdev/xiaodu/suoni/collect_tag/" + args[0];
        this.taskNum = 20;
    }

    @Override
    public void reduce(Text key, Iterator<LongWritable> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        long sum = 0;
        while (values.hasNext()) {
            sum += values.next().get();
        }
        if (sum > 20)
            output.collect(key, Utils.mergeKey(str(sum)));
    }

    @Override
    public void map(Object key, Text value, OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException {
        String[] _allCols = StringUtils.splitPreserveAllTokens(value.toString(), TAB);
        if (_allCols.length < 11) {
            return;
        }

        String beginDate = format(add(inputArgs[0], -30)) + " 00:00:00";
        if (_allCols[CollectInfo.collect_time].compareTo(beginDate) >= 0 && CATS.contains(_allCols[CollectInfo.category])) {
            String tags = _allCols[CollectInfo.tag];
            if (StringUtils.isNotEmpty(tags)) {
                for (String tag : StringUtils.split(tags, ' ')) {
                    output.collect(Utils.mergeKey(_allCols[CollectInfo.item_type], tag), ONE);
                }
            }
        }
    }

    static List<String> CATS = Arrays.asList(("162103\n" +
            "162104\n" +
            "162116\n" +
            "50000852\n" +
            "50000671\n" +
            "50000697\n" +
            "1623\n" +
            "1624\n" +
            "1629\n" +
            "50013194\n" +
            "50013196\n" +
            "50008899\n" +
            "50008906\n" +
            "50008900\n" +
            "50008904\n" +
            "50010850\n" +
            "16\n" +
            "50008905\n" +
            "50008898\n" +
            "50011277\n" +
            "162105\n" +
            "162205\n" +
            "1622\n" +
            "50008897\n" +
            "50008901\n" +
            "50011404").split("\n"));
}
