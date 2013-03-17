package org.dueam.hadoop.jobs.mall;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.LongSumReducer;
import org.apache.hadoop.util.ToolRunner;
import org.dueam.hadoop.common.MapReduce;
import org.dueam.hadoop.common.util.Utils;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * User: windonly
 * Date: 11-4-22 ÉÏÎç11:13
 */
public class MallOnlineItemList extends MapReduce<Text, LongWritable> {

    @Override
    protected void setup(String[] args) {
        this.input = new String[]{"/group/tbptd-dev/xiaodu/hive/mall_online_item_with_cm_conditions/pt=" + args[0] + "000000/cm_type=1", "/group/tbptd-dev/xiaodu/hive/mall_online_item_with_cm_conditions/pt=" + args[0] + "000000/cm_type=2"};
        this.inputFormat = TextInputFormat.class;
        this.output = "/group/tbdev/xiaodu/suoni/mall_online_item_with_cm_conditions/" + args[0];
    }


    @Override
    protected void beforeRun(String[] args, JobConf config) {
        config.setCombinerClass(LongSumReducer.class);
    }

    public void reduce(Text key, Iterator<LongWritable> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        long sum = 0;
        while (values.hasNext()) {
            sum = sum + values.next().get();
        }
        if (sum > 50)
            output.collect(key, new Text(String.valueOf(sum)));
    }

    public static class TempTable {
        public final static int auction_id = 0;
        public final static int spu_id = 1;
        public final static int sold_quantity = 2;
        public final static int reserve_price = 3;
        public final static int property = 4;
        public final static int user_id = 5;
        public final static int category = 6;
        public final static int cm_cat_conditions = 7;
        public final static int cat_type = 8;
    }

    public void map(Object key, Text value, OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException {
        String[] _allCols = StringUtils.splitPreserveAllTokens(value.toString(), (char) 0x09);
        if (_allCols.length < 8) return;

        String conditions = _allCols[TempTable.cm_cat_conditions];
        String property = _allCols[TempTable.property];
        String category = _allCols[TempTable.category];
        String spuId = _allCols[TempTable.spu_id];
        String userId = _allCols[TempTable.user_id];
        String catType = _allCols[TempTable.cat_type];
        if (!isMatchCategory(spuId, property, conditions)) {
            output.collect(Utils.mergeKey(catType, "TOP_SELLER", userId), ONE);
            output.collect(Utils.mergeKey(catType, "TOP_CATEGORY", category), ONE);
        }


    }

    public static boolean isMatchCategory(String spuId, String property, String conditions) {
        if (StringUtils.isBlank(conditions)) return false;
        if (conditions.indexOf('S') >= 0) {
            if (conditions.indexOf("S" + spuId) >= 0) {
                return true;
            }
        }
        for (String condition : StringUtils.splitPreserveAllTokens(conditions, '#')) {
            String[] array = StringUtils.splitPreserveAllTokens(condition, '$');
            if (null == array || array.length < 2) continue;
            for (String subCondition : StringUtils.splitPreserveAllTokens(array[1], '|')) {
                if (StringUtils.isBlank(subCondition)) continue;
                if (subCondition.indexOf('P') > 0) {
                    for (String prop : StringUtils.splitPreserveAllTokens(subCondition, ';')) {
                        if (prop.startsWith("P")) {
                            Set<String> _tmpSet = new HashSet<String>();
                            String pid = prop.substring(1, prop.indexOf(':'));
                            for (String vid : StringUtils.splitPreserveAllTokens(prop.substring(prop.indexOf(':') + 1), ',')) {
                                _tmpSet.add(pid + ":" + vid);
                            }
                            if (StringUtils.indexOfAny(property, _tmpSet.toArray(new String[0])) >= 0) {
                                return true;
                            }
                        }
                    }
                } else {
                    return true;
                }
            }
        }

        return false;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new MallOnlineItemList(),
                args);
        System.exit(res);
    }
}
