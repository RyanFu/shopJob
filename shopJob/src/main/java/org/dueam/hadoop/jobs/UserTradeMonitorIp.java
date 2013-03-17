package org.dueam.hadoop.jobs;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ToolRunner;
import org.dueam.hadoop.common.Area;
import org.dueam.hadoop.common.CounterMap;
import org.dueam.hadoop.common.HadoopTable;
import org.dueam.hadoop.common.MapReduce;
import org.dueam.hadoop.common.tables.TcBizOrder;
import org.dueam.hadoop.common.util.Utils;

import java.io.IOException;
import java.util.Iterator;

/**
 * 用户交易监控报表
 * User: windonly
 * Date: 10-12-16 下午5:44
 */
public class UserTradeMonitorIp extends MapReduce<Text, Text> {
    @Override
    protected void setup(String[] args) {
        this.taskNum = 200;
        this.input = HadoopTable.order(args[0], "200").getInputPath();
        this.inputFormat = HadoopTable.order(null).getInputFormat();
        this.output = "/group/tbbp/suoni/user_trade_monitor_ip/" + args[0];
    }

    @Override
    protected void beforeRun(String[] args, JobConf config) {
        config.setMapOutputValueClass(Text.class);
    }

    @Override
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
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
    }

    @Override
    public void map(Object key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        String[] _allCols = StringUtils.splitPreserveAllTokens(value.toString(), TAB);
        if (_allCols.length < 47) {
            return;
        }

        if (TcBizOrder.isPaied(_allCols) && TcBizOrder.isDetail(_allCols)) {
            String buyerId = _allCols[TcBizOrder.BUYER_ID];
            String payTime = StringUtils.substring(_allCols[TcBizOrder.PAY_TIME], 11, 13);
            String priceArea = alipayTradeArea.getArea(NumberUtils.toLong(_allCols[TcBizOrder.AUCTION_PRICE]));
            String rootCatId = Utils.getValue(_allCols[TcBizOrder.ATTRIBUTES], "realRootCat", "NULL");
            output.collect(new Text(buyerId), Utils.mergeKey(rootCatId, priceArea, payTime));
        }
    }


    static Area alipayTradeArea = Area.newArea(Area.MoneyCallback, 0, 1 * 100, 10 * 100, 50 * 100, 100 * 100, 200 * 100, 300 * 100, 500 * 100, 10000 * 100, 100000 * 100);


    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),
                new UserTradeMonitorIp(), args);
        System.exit(res);
    }


}
