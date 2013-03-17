package org.dueam.hadoop.jobs;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.LongSumReducer;
import org.apache.hadoop.util.ToolRunner;
import org.dueam.hadoop.common.HadoopTable;
import org.dueam.hadoop.common.MapReduce;
import org.dueam.hadoop.common.Table;
import org.dueam.hadoop.common.tables.ISearchLog;
import org.dueam.hadoop.common.tables.Sku;
import org.dueam.hadoop.common.util.DateStringUtils;
import org.dueam.hadoop.common.util.Utils;

import java.io.IOException;
import java.util.*;

/**
 * User: windonly
 * Date: 11-4-7 ÉÏÎç9:36
 */
public class ISearch extends MapReduce<Text, LongWritable> {
    @Override
    protected void setup(String[] args) {
        this.input = new String[]{"/group/tbptd-dev/xiaodu/isearch"};
        this.inputFormat = TextInputFormat.class;
        this.output = "/group/tbptd-dev/xiaodu/isearch_result";
    }

    protected String outputTemp = null;

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 1) {
            args = new String[]{DateStringUtils.now()};
            System.out.println("ERROR: Please Enter Date , eg. 20101010 ! now use default => " + DateStringUtils.now());
        }
        setup(args);
        if (true) {
            JobConf config = new JobConf(getConf(), getClass());
            config.set("user.args", Utils.asString(args));

            config.setJobName(getName() + "-" + System.currentTimeMillis());
            config.setNumReduceTasks(45);
            config.setMapperClass(getClass());
            config.setReducerClass(LongSumReducer.class);
            config.setCombinerClass(LongSumReducer.class);
            config.setInputFormat(inputFormat);
            config.setMapOutputKeyClass(Text.class);
            config.setMapOutputValueClass(LongWritable.class);

            //add input paths
            for (String path : input) {
                if (TextInputFormat.class.equals(inputFormat)) {
                    TextInputFormat.addInputPath(config, new Path(path));
                } else if (SequenceFileInputFormat.class.equals(inputFormat)) {
                    SequenceFileInputFormat.addInputPath(config, new Path(path));
                }
            }

            config.setOutputKeyClass(Text.class);
            config.setOutputValueClass(Text.class);

            //if output path exists then return
            FileSystem fs = FileSystem.get(config);
            Path outputPath = new Path(output);
            FileOutputFormat.setOutputPath(config, outputPath);

            if (!fs.exists(outputPath)) {
                JobClient.runJob(config);
            } else {
                System.out.println("You has finished this job today ! " + outputPath);
            }

        }


        return JobClient.SUCCESS;
    }

    @Override
    public void reduce(Text key, Iterator<LongWritable> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

    }

    @Override
    public void map(Object key, Text value, OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException {
        String[] _allCols = StringUtils.splitPreserveAllTokens(value.toString(), ISearchLog.SPLIT_CHAR);
        if (_allCols.length < 7) {
            return;
        }
        long runTime = NumberUtils.toLong(_allCols[ISearchLog.RUN_TIME]);
        long queryTime = NumberUtils.toLong(_allCols[ISearchLog.QUERY_TIME]);
        output.collect(Utils.mergeKey("sum", "total"), ONE);
        output.collect(Utils.mergeKey("sum", "runTime"), new LongWritable(runTime));
        output.collect(Utils.mergeKey("sum", "queryTime"), new LongWritable(queryTime));
        Set<String> querySet = new HashSet<String>();

        for (String kv : StringUtils.splitPreserveAllTokens(_allCols[ISearchLog.QUERY_URL], '&')) {
            String[] array = StringUtils.splitPreserveAllTokens(kv, '=');
            if (array.length < 2) continue;
            String _key = array[0];
            String _value = array[1];
            if ("filter".equals(_key)) {
                String[] _array = StringUtils.splitPreserveAllTokens(_value, ':');
                if (_array.length > 0) {
                    String __key = _array[0];
                    if (__key.indexOf('[') >= 0) {
                        __key = __key.substring(0, __key.indexOf('['));
                    }
                    //output.collect(Utils.mergeKey("filter", __key), ONE);
                    if (queryParam.contains(__key))
                        querySet.add("filter:" + __key);
                }
                //_array[0]
            } else if ("src".equals(_key)) {
                if (_value.indexOf('-') >= 0)
                    output.collect(Utils.mergeKey("src", _value.substring(0, _value.lastIndexOf('-'))), ONE);
            } else {
                if (queryParam.contains(_key))
                        querySet.add(_key);
                //output.collect(new Text(_key), ONE);
            }

        }
        List<String> _list = new ArrayList<String>();
        _list.addAll(querySet);
        Collections.sort(_list);
        output.collect(Utils.mergeKey("query", StringUtils.join(_list,'|')), ONE);
    }

    public static String[] input = ("ps\n" +
            "ss\n" +
            "spu_id\n" +
            "q\n" +
            "scatid\n" +
            "s_cat\n" +
            "options\n" +
            "postage_id\n" +
            "queryType\n" +
            "_options\n" +
            "_ps\n" +
            "nk\n" +
            "_ss\n" +
            "prc\n" +
            "promoted_status\n" +
            "cat\n" +
            "_auction_id\n" +
            "ad_id\n" +
            "auction_id\n" +
            "outfmt\n" +
            "username\n" +
            "outer_id\n" +
            "am_id\n" +
            "wwdialog").split("\n");
    public static Set<String> queryParam = new HashSet<String>();

    static {
        for (String _tmp : input)
            queryParam.add(_tmp);
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new ISearch(),
                args);
        System.exit(res);
    }
}
