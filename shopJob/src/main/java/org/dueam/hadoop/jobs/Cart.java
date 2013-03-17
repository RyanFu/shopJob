package org.dueam.hadoop.jobs;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.dueam.hadoop.common.HadoopTable;
import org.dueam.hadoop.common.SimpleMapReduce;
import org.dueam.hadoop.common.util.DateStringUtils;
import org.dueam.hadoop.common.util.Utils;

import static org.dueam.hadoop.common.tables.Cart.*;

import java.io.IOException;
import static org.dueam.hadoop.conf.Cart.*;

/**
 * User: windonly
 * Date: 11-1-6 下午4:45
 */
public class Cart extends SimpleMapReduce {
    @Override
    protected String[] getInputPath(String[] args) {
        return new String[]{HadoopTable.cart(args[0])};
    }

    @Override
    protected Class getInputFormat() {
        return TextInputFormat.class;
    }

    @Override
    protected String getOutputPath(String[] args) {
        return "/group/tbdev/xiaodu/suoni/cart/" + args[0];

    }

    @Override
    protected void doWork(String line, OutputCollector<Text, LongWritable> output) throws IOException {
        String[] _allCols = StringUtils.splitPreserveAllTokens(line, TAB);
        String queryDate = DateStringUtils.format(inputArgs[0]);
        if (isDeleted(_allCols) && Utils.isSameDay(_allCols[GMT_MODIFIED], queryDate)) {
            output.collect(Utils.mergeKey("del", _allCols[STATUS]), new LongWritable(1));
            //最后一次加入购物车的时间到下单的时间统计
            if ("-1".equals(_allCols[STATUS])) {
                long second = DateStringUtils.second(_allCols[LAST_MODIFIED], _allCols[GMT_MODIFIED]);
                output.collect(Utils.mergeKey("avg_last", "sum"), new LongWritable(second));
                output.collect(Utils.mergeKey("avg_last", "count"), new LongWritable(1));
                output.collect(Utils.mergeKey("last", getArea(second)), new LongWritable(1));
                if(Utils.isSameDay(_allCols[LAST_MODIFIED],_allCols[GMT_MODIFIED])){
                    output.collect(Utils.mergeKey("today_order", "last"), new LongWritable(1));
                }
            }
            //第一次加入购物车的时间到下单的时间统计
            if ("-1".equals(_allCols[STATUS])) {
                long second = DateStringUtils.second(_allCols[GMT_CREATE], _allCols[GMT_MODIFIED]);
                output.collect(Utils.mergeKey("avg_order", "sum"), new LongWritable(second));
                output.collect(Utils.mergeKey("avg_order", "count"), new LongWritable(1));
                output.collect(Utils.mergeKey("order", getArea(second)), new LongWritable(1));
                 if(Utils.isSameDay(_allCols[GMT_CREATE],_allCols[GMT_MODIFIED])){
                    output.collect(Utils.mergeKey("today_order", "created"), new LongWritable(1));
                }
            }

        }
        if (Utils.isSameDay(_allCols[GMT_CREATE], queryDate)) {
            output.collect(Utils.mergeKey("sum", "created"), new LongWritable(1));
        }
        if (Utils.isSameDay(_allCols[GMT_MODIFIED], queryDate)) {
            output.collect(Utils.mergeKey("sum", "modified"), new LongWritable(1));
        }

    }



    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),
                new Cart(), args);
        System.exit(res);
    }
}
