package org.dueam.hadoop.jobs;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.dueam.hadoop.common.HadoopTable;
import org.dueam.hadoop.common.SimpleMapReduce;
import org.dueam.hadoop.common.tables.Atpanel;
import org.dueam.hadoop.common.util.ItemUtils;
import org.dueam.hadoop.common.util.Utils;

import java.io.IOException;

/**
 * User: windonly
 * Date: 11-1-4 ÏÂÎç1:14
 */
public class ItemDetail extends SimpleMapReduce {
    @Override
    protected String[] getInputPath(String[] args) {
        String[] input = new String[]{HadoopTable.atpanel(args[0])};
        System.out.println("input path => " + Utils.asString(input));
        return input;
    }

    @Override
    protected Class getInputFormat() {
        return SequenceFileInputFormat.class;
    }

    @Override
    protected String getOutputPath(String[] args) {
        return "/group/tbdev/xiaodu/suoni/item_detail/" + args[0];
    }

    @Override
    protected void doWork(String line, OutputCollector<Text, LongWritable> output) throws IOException {
        String[] _allCols = StringUtils.splitPreserveAllTokens(line, '"');
        if (_allCols.length < 8) return;
        String url = _allCols[Atpanel.url];
        if(!_allCols[Atpanel.refer].startsWith("/1.gif?")) {
            return ;
        }

        String id = getId(url);
        if (id != null) {
            if (StringUtils.isNumeric(id) && NumberUtils.toLong(id) > 0) {
                String key = String.valueOf(NumberUtils.toLong(id) % 8192);
                output.collect(new Text(key), new LongWritable(1));
            }
        }
    }

    private static String getId(String url) {

        return ItemUtils.getId(url);
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),
                new ItemDetail(), args);
        System.exit(res);
    }
}
