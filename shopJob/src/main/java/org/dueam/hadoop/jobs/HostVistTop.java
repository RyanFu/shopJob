package org.dueam.hadoop.jobs;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.dueam.hadoop.common.HadoopTable;
import org.dueam.hadoop.common.tables.Atpanel;
import org.dueam.hadoop.common.util.DateStringUtils;
import org.dueam.hadoop.common.util.Utils;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * 域名同时在线链接（URL）
 * User: windonly
 * Date: 11-1-4 下午1:14
 */
public class HostVistTop extends Configured implements Tool, Reducer<Text, Text, Text, Text>, Mapper<Object, Text, Text, Text> {
    public int run(String[] args) throws Exception {
        if (args.length < 1) {
            args = new String[]{DateStringUtils.now()};
            System.out.println("ERROR: Please Enter Date , eg. 20101010 ! now use default => " + DateStringUtils.now());
        }

        JobConf config = new JobConf(getConf(), getClass());
        config.set("user.args", Utils.asString(args));

        config.setJobName(getClass() + "-" + System.currentTimeMillis());
        config.setNumReduceTasks(25);
        config.setMapperClass(getClass());
        config.setReducerClass(getClass());
        config.setInputFormat(getInputFormat());
        config.setMapOutputKeyClass(Text.class);
        config.setMapOutputValueClass(Text.class);

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

        if (!fs.exists(outputPath)) {
            JobClient.runJob(config);
        } else {
            System.out.println("You has finished this job today ! " + outputPath);
        }

        return JobClient.SUCCESS;
    }

    protected String[] getInputPath(String[] args) {
        String[] input = new String[]{HadoopTable.atpanel(args[0])};
        System.out.println("input path => " + Utils.asString(input));
        return input;
    }

    protected Class getInputFormat() {
        return SequenceFileInputFormat.class;
    }

    protected String getOutputPath(String[] args) {
        return "/group/tbdev/xiaodu/suoni/host_vist_top/" + args[0];
    }

    public void map(Object key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        doWork(value.toString(), output);
    }

    protected String[] inputArgs;

    public void configure(JobConf job) {
        this.inputArgs = Utils.asArray(job.get("user.args"));
    }

    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        Set<String> ipSet = new HashSet<String>();
        long count = 0;
        while (values.hasNext()) {
            count++;
            ipSet.add(values.next().toString());
        }
        if (ipSet.size() < 10) return;
        output.collect(key, new Text(count + "\t" + ipSet.size()));

    }

    protected void doWork(String line, OutputCollector<Text, Text> output) throws IOException {
        String[] _allCols = StringUtils.splitPreserveAllTokens(line, '"');
        if (_allCols.length < 8) return;
        String url = _allCols[Atpanel.url];
        if (!_allCols[Atpanel.refer].startsWith("/1.gif?")) {
            return;
        }


        String userId = _allCols[Atpanel.uid];
        if (NumberUtils.toLong(userId, 0) < 1) {
            return;
        }


        String mid = _allCols[Atpanel.mid];

        if (StringUtils.isEmpty(mid)) {
            return;
        }

        String host = Utils.getHost(url);

        String time = _allCols[Atpanel.time];
        output.collect(new Text(host + "\t" + time.substring(0, 12) + "00"), new Text(mid));


    }

    static String[] KEY_PAGES = ("http://i.taobao.com/my_taobao.htm\n" +
            "http://trade.taobao.com/trade/itemlist/list_bought_items.htm\n" +
            "http://trade.taobao.com/trade/itemlist/list_sold_items.htm\n" +
            "http://sell.taobao.com/auction/goods/goods_in_stock.htm\n" +
            "http://sell.taobao.com/auction/goods/goods_on_sale.htm\n" +
            "http://buy.taobao.com/auction/buy_now.jhtml\n" +
            "http://buy.taobao.com/auction/order/confirm_order.htm").split("\n");

    public static boolean isKeyPage(String url) {
        for (String page : KEY_PAGES) {
            if (StringUtils.indexOf(url, page) >= 0) {
                return true;
            }
        }
        return false;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),
                new HostVistTop(), args);
        System.exit(res);
    }


    public void close() throws IOException {

    }
}
