package org.dueam.hadoop.jobs;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.dueam.hadoop.common.HadoopTable;
import org.dueam.hadoop.common.tables.Atpanel;
import org.dueam.hadoop.common.util.DateStringUtils;
import org.dueam.hadoop.common.util.MapUtils;
import org.dueam.hadoop.common.util.Utils;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * User: windonly
 * Date: 11-1-4 ÏÂÎç1:14
 */
public class UserViewLogFilterTwo extends Configured implements Tool, Reducer<Text, Text, Text, Text>, Mapper<Object, Text, Text, Text> {

    public int run(String[] args) throws Exception {
        if (args.length < 1) {
            args = new String[]{DateStringUtils.now()};
            System.out.println("ERROR: Please Enter Date , eg. 20101010 ! now use default => " + DateStringUtils.now());
        }

        JobConf config = new JobConf(getConf(), getClass());
        config.set("user.args", Utils.asString(args));

        config.setJobName(getClass() + "-" + System.currentTimeMillis());
        config.setNumReduceTasks(100);
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
        String[] input = new String[]{"/group/tbdev/xiaodu/suoni/user_view_log_filter/" + args[0]};
        System.out.println("input path => " + Utils.asString(input));
        return input;
    }

    protected Class getInputFormat() {
        return TextInputFormat.class;
    }

    protected String getOutputPath(String[] args) {
        return "/group/tbdev/xiaodu/suoni/user_view_log_filter2/" + args[0];
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
        Set<String> midSet = new HashSet<String>();
        StringBuffer _outBuffer = new StringBuffer();
        while (values.hasNext()) {
            String line = values.next().toString();
            String[] _allCols = StringUtils.splitPreserveAllTokens(line, '|');
            midSet.add(_allCols[2]);
            ipSet.add(_allCols[3]);
            if (_outBuffer.indexOf(line) < 0)
                _outBuffer.append(line).append('#');
        }
        if (ipSet.size() > 1 && midSet.size() > 1) {
            if (_outBuffer.length() > 0) {
                _outBuffer.deleteCharAt(_outBuffer.length() - 1);
            }
            output.collect(key, new Text(_outBuffer.toString()));
        }
    }

    protected void doWork(String line, OutputCollector<Text, Text> output) throws IOException {
        String[] _allColsTemp = StringUtils.splitPreserveAllTokens(line, (char) 0x09);
        String[] _allCols = StringUtils.splitPreserveAllTokens(_allColsTemp[1], '"');
        if (_allCols.length < 8) return;
        String url = _allCols[Atpanel.url];
        if (!_allCols[Atpanel.refer].startsWith("/1.gif?")) {
            return;
        }

        String userId = _allCols[Atpanel.uid];
        if (NumberUtils.toLong(userId, 0) < 1) {
            return;
        }
        if (!isKeyPage(url)) {
            return;
        }
        long tradeId = 0;
        int type = 0;
        if (isOrderPage(url)) {
            Map<String, String> refer = MapUtils.asMap(_allCols[Atpanel.refer]);
            tradeId = NumberUtils.toLong(refer.get("tradeid"));
            type = 1;//order
        } else if (isPayPage(url)) {
            if (url.indexOf('?') > 0) {
                Map<String, String> refer = MapUtils.asMap(url.substring(url.indexOf('?') + 1));
                tradeId = NumberUtils.toLong(refer.get("biz_order_id"));
                type = 2;//pay
            }
        }


        if (tradeId < 1) return;


        output.collect(Utils.mergeKey(userId,String.valueOf(tradeId)), new Text(type +"|" + _allCols[Atpanel.time] + "|" + _allCols[Atpanel.mid] + "|" + _allCols[Atpanel.ip]));


    }

    static String[] KEY_PAGES = ("http://buy.taobao.com/auction/order/confirm_order.htm\n" +
            "http://buy.taobao.com/auction/buy_now.htm\n" +
            "http://buy.taobao.com/auction/fastbuy/fast_buy.htm\n" +
            "http://trade.taobao.com/trade/pay_success.htm").split("\n");

    public static boolean isKeyPage(String url) {
        for (String page : KEY_PAGES) {
            if (StringUtils.indexOf(url, page) >= 0) {
                return true;
            }
        }
        return false;
    }

    static String[] PAY_PAGES = (
            "http://trade.taobao.com/trade/pay_success.htm").split("\n");

    public static boolean isPayPage(String url) {
        for (String page : PAY_PAGES) {
            if (StringUtils.indexOf(url, page) >= 0) {
                return true;
            }
        }
        return false;
    }

    static String[] ORDER_PAGES = (
            "http://buy.taobao.com/auction/order/confirm_order.htm\n" +
                    "http://buy.taobao.com/auction/buy_now.htm\n" +
                    "http://buy.taobao.com/auction/fastbuy/fast_buy.htm").split("\n");

    public static boolean isOrderPage(String url) {
        for (String page : ORDER_PAGES) {
            if (StringUtils.indexOf(url, page) >= 0) {
                return true;
            }
        }
        return false;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),
                new UserViewLogFilterTwo(), args);
        System.exit(res);
    }


    public void close() throws IOException {

    }
}
