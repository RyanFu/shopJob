package org.dueam.hadoop.jobs.mall;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.dueam.hadoop.common.HadoopTable;
import org.dueam.hadoop.common.MapReduce;
import org.dueam.hadoop.common.SimpleMapReduce;
import org.dueam.hadoop.common.tables.CmCategories;
import org.dueam.hadoop.common.util.Utils;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * User: windonly
 * Date: 11-4-22 上午11:13
 */
public class CmCategorySplitByConditions extends MapReduce<Text, Text> {

    @Override
    protected void setup(String[] args) {
        this.input = HadoopTable.cmCategory(args[0]).getInputPath();
        this.inputFormat = HadoopTable.cmCategory(null).getInputFormat();
        this.output = "/group/tbdev/xiaodu/suoni/cm_category_split_by_conditions/" + args[0];
    }

    @Override
    protected void beforeRun(String[] args, JobConf config) {
        config.setMapOutputValueClass(Text.class);
    }

    @Override
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        Set<String> valueSet = new HashSet<String>();
        while (values.hasNext()) {
            valueSet.add(values.next().toString());
        }
        output.collect(key, new Text(StringUtils.join(valueSet, "#")));
    }


    public void map(Object key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        String[] _allCols = StringUtils.splitPreserveAllTokens(value.toString(), CmCategories.SPLIT_CHAR);
        if (_allCols.length < 20) return;
        String catId = _allCols[CmCategories.cat_id];
        String conditions = _allCols[CmCategories.conditions];
        String catType = _allCols[CmCategories.cat_type];
        //只处理第一、第二套前台类目（有效的）
        if (!CmCategories.isEffective(_allCols) || !CmCategories.isCatType(_allCols, CmCategories.CAT_TYPE_C2C, CmCategories.CAT_TYPE_B2C)) {
            return;
        }
        if (StringUtils.isNotBlank(conditions)) {
            for (String condition : StringUtils.splitPreserveAllTokens(conditions, '|')) {
                if (condition.startsWith("C")) {
                    String category = null;
                    if (condition.indexOf(';') > 0) {
                        category = condition.substring(1, condition.indexOf(';'));
                    } else {
                        category = condition.substring(1);
                    }
                    if (StringUtils.isNotBlank(category)) {
                        output.collect(Utils.mergeKey(catType, "1", category), new Text(catId+"$"+conditions));
                    }
                }

                if (condition.startsWith("S")) {
                    String spu = null;
                    if (condition.indexOf(';') > 0) {
                        spu = condition.substring(1, condition.indexOf(';'));
                    } else {
                        spu = condition.substring(1);
                    }
                    if (StringUtils.isNotBlank(spu)) {
                        output.collect(Utils.mergeKey(catType, "2", spu),new Text(catId+"$"+conditions));
                    }
                }
            }
        }

    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new CmCategorySplitByConditions(),
                args);
        System.exit(res);
    }
}
