package org.dueam.hadoop.jobs;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ToolRunner;
import org.dueam.hadoop.common.HadoopTable;
import org.dueam.hadoop.common.KeyValuePair;
import org.dueam.hadoop.common.MapReduce;
import org.dueam.hadoop.common.tables.TcBizOrder;
import org.dueam.hadoop.common.util.DateStringUtils;
import org.dueam.hadoop.common.util.Utils;

import java.io.IOException;
import java.nio.Buffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.dueam.hadoop.common.Functions.newArrayList;
import static org.dueam.hadoop.common.Functions.str;

/**
 * 统计所有的宝贝的价格变化情况
 * User: windonly
 * Date: 10-12-16 下午5:44
 */
public class AuctionPrice extends MapReduce<Text, Text> {
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

        //处理退款相关
        if (TcBizOrder.isDetail(_allCols) && TcBizOrder.isPaied(_allCols)) {
            String attr = _allCols[TcBizOrder.ATTRIBUTES];
            String cat = Utils.getValue(attr, "realRootCat", null);
            if (!"16".equals(cat)) return;
            long price = (long) NumberUtils.toDouble(_allCols[TcBizOrder.AUCTION_PRICE]);
            String day = DateStringUtils.getDate(gmtCreated);
            String skuId = Utils.getValue(attr, "sku", "0");
            skuId = StringUtils.substringBefore(skuId, "|");
            long discountFee = (long)(NumberUtils.toDouble(_allCols[TcBizOrder.DISCOUNT_FEE]) / NumberUtils.toDouble(_allCols[TcBizOrder.BUY_AMOUNT])) ;
            output.collect(Utils.mergeKey(_allCols[TcBizOrder.AUCTION_ID], skuId), Utils.mergeKey(gmtCreated, str(price - discountFee )));

        }


    }


    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),
                new AuctionPrice(), args);
        System.exit(res);
    }


    @Override
    protected void setup(String[] args) {
        this.input = HadoopTable.order(args[0], "50").getInputPath();
        this.inputFormat = HadoopTable.order(args[0], "50").getInputFormat();
        this.output = "/group/tbdev/xiaodu/suoni/auction_price/" + args[0];
        this.taskNum = 100;
    }

    @Override
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

        List<KeyValuePair> priceList = newArrayList();
        while (values.hasNext()) {
            String[] array = StringUtils.splitPreserveAllTokens(values.next().toString(), TAB);

            long price = NumberUtils.toLong(array[1]);
            putPrice(priceList, array[0], price);
        }
        Collections.sort(priceList);
        StringBuffer message = new StringBuffer();
        if (doFilter(priceList, message))
            output.collect(key, Utils.mergeKey(message.toString()));
    }


    public static String toString(List<KeyValuePair> priceList) {
        StringBuffer buffer = new StringBuffer();
        for (KeyValuePair pair : priceList) {
            buffer.append(pair.getKey()).append(',').append(pair.getValue()).append(',').append(pair.getExtValue()).append('|');
        }
        if (buffer.length() > 0) {
            buffer.deleteCharAt(buffer.length() - 1);
        }
        return buffer.toString();
    }

    //auction_id,sku_id,sold_sum,up_count,down_count,details
    public static boolean doFilter(List<KeyValuePair> priceList, StringBuffer message) {
        long sum = 0;
        for (KeyValuePair pair : priceList) {
            sum += pair.getExtValue();
        }
        //只对销量过百的商品做统计
        if (sum < 30) return false;
        message.append(sum).append(TAB);
        //message.append("sold_sum:").append(sum).append("|");
        long currPrice = 0;
        long up = 0, down = 0;
        long currSum = 0;
        StringBuffer _message = new StringBuffer();
        for (KeyValuePair pair : priceList) {
            if (currPrice > 0) {
                long cPrice = pair.getValue() - currPrice;
                //价格浮动在10%以上的变更
                if (Math.abs(cPrice * 1000 / Math.min(currPrice,pair.getValue())) > 100) {
                    //涨价

                    if ((up + down) > 0) {
                        _message.append(':').append(currSum).append('|');
                    } else {
                        _message.append((double) currPrice / 100).append(':').append(currSum).append('|');
                    }
                    //_message.append((double) cPrice / 100).append("(").append((double) currPrice / 100).append("->").append((double) pair.getValue() / 100).append(')');
                    _message.append((double) pair.getValue() / 100).append("(").append((double) currPrice / 100).append(cPrice>0?'+':'-').append((double) Math.abs(cPrice) / 100).append(')');
                    currSum = 0;
                    if (cPrice > 0) {
                        up++;
                        //message.append(DateStringUtils.getDate(pair.getKey())).append(":up:").append(cPrice).append(":from:").append(currPrice).append(":to:").append(pair.getValue()).append(":sold:").append(pair.getExtValue()).append(":before_sold:").append(currSum).append("|");
                    } else {
                        down++;
                        //message.append(DateStringUtils.getDate(pair.getKey())).append(":down:").append(cPrice).append(":from:").append(currPrice).append(":to:").append(pair.getValue()).append(":sold:").append(pair.getExtValue()).append(":before_sold:").append(currSum).append("|");
                    }
                }
            }
            currSum += pair.getExtValue();
            currPrice = pair.getValue();
        }


        if ((up + down) > 0 && up > 0) {
            //message.append("sum:up:").append(up).append(":down:").append(down);
            _message.append(':').append(currSum).append('|');

        }
        message.append(up).append(TAB).append(down).append(TAB);
        message.append(_message.toString());
        if ((up + down) > 1) return true;

        return false;
    }

    private static void putPrice(List<KeyValuePair> priceList, String time, long price) {
        if (price <= 0) return;
        String day = DateStringUtils.getDate(time);
        for (KeyValuePair _tmp : priceList) {
            //如果相等
            if (StringUtils.indexOf(_tmp.getKey(), day) >= 0) {
                //如果价格幅度不超过1%
                if ((Math.abs(price - _tmp.getValue()) * 1000 / _tmp.getValue()) < 1) {
                    _tmp.setExtValue(_tmp.getExtValue() + 1);
                    return;
                }
            }
        }
        priceList.add(KeyValuePair.newPair(time, price, 1, KeyValuePair.DateKey));
    }

    @Override
    public void map(Object key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        doWork(value.toString(), output);
    }
}
