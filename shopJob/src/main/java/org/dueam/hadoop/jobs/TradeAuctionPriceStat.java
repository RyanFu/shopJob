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
import org.dueam.hadoop.common.HadoopTable;
import org.dueam.hadoop.common.MapReduce;
import org.dueam.hadoop.common.SimpleMapReduce;
import org.dueam.hadoop.common.tables.TcBizOrder;
import org.dueam.hadoop.common.util.DateStringUtils;
import org.dueam.hadoop.common.util.Utils;

import java.io.IOException;
import java.security.Key;
import java.util.Iterator;

import static org.dueam.hadoop.common.Functions.str;

/**
 * 统计某一大类下面各个叶子类目的商品的价格分布(最近50天)
 * User: windonly
 * Date: 10-12-16 下午5:44
 */
public class TradeAuctionPriceStat extends MapReduce<Text, Text> {
    @Override
    protected void beforeRun(String[] args, JobConf config) {
        config.setMapOutputValueClass(Text.class);

    }

    protected void doWork(String line, OutputCollector<Text, Text> output) throws IOException {
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

        String rootCatPara = "16";
        if (inputArgs.length > 1) {
            rootCatPara = inputArgs[1];
        }

        //处理退款相关
        if (TcBizOrder.isDetail(_allCols) && TcBizOrder.isPaied(_allCols) && !TcBizOrder.isB2C(_allCols)) {
            String attr = _allCols[TcBizOrder.ATTRIBUTES];
            long price = (long) NumberUtils.toDouble(_allCols[TcBizOrder.AUCTION_PRICE]);
            String rootCat = Utils.getValue(attr, "realRootCat", "NULL");

            if (StringUtils.equals(rootCatPara, rootCat)) {
                String cat = Utils.getValue(attr, "rootCat", null);
                if (StringUtils.isNotEmpty(cat)) {
                    output.collect(Utils.mergeKey(cat, str(price / 1000)), Utils.mergeKey("1", str(TcBizOrder.getTotalFee(_allCols))));
                }
            }

        }


    }


    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),
                new TradeAuctionPriceStat(), args);
        System.exit(res);
    }


    @Override
    protected void setup(String[] args) {
        this.input = HadoopTable.order(args[0], "50").getInputPath();
        this.inputFormat = HadoopTable.order(args[0], "50").getInputFormat();
        this.output = "/group/tbdev/xiaodu/suoni/trade_auction_price_stat/" + args[0];
    }

    @Override
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        long count = 0;
        long totalFee = 0;
        while (values.hasNext()) {
            String[] array = StringUtils.splitPreserveAllTokens(values.next().toString(), TAB);
            count += NumberUtils.toLong(array[0]);
            totalFee += NumberUtils.toLong(array[1]);
        }
        output.collect(key, Utils.mergeKey(str(count), str(totalFee)));
    }

    @Override
    public void map(Object key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        doWork(value.toString(), output);
    }
}
