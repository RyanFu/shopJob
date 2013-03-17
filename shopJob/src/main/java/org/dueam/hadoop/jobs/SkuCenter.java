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
import org.dueam.hadoop.common.tables.Sku;
import org.dueam.hadoop.common.util.DateStringUtils;
import org.dueam.hadoop.common.util.Utils;

import java.io.IOException;
import java.util.Iterator;

/**
 * User: windonly
 * Date: 11-4-7 ÉÏÎç9:36
 */
public class SkuCenter extends MapReduce<Text, LongWritable> {
    @Override
    protected void setup(String[] args) {
        Table sku = HadoopTable.dbSku(args[0]);
        this.input = sku.getInputPath();
        this.inputFormat = sku.getInputFormat();
        this.output = "/group/tbdev/xiaodu/suoni/sku_center/" + args[0];
        this.outputTemp = "/group/tbdev/xiaodu/suoni/sku_center_temp/" + args[0];
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
            Path outputPath = new Path(outputTemp);
            FileOutputFormat.setOutputPath(config, outputPath);

            if (!fs.exists(outputPath)) {
                JobClient.runJob(config);
            } else {
                System.out.println("You has finished this job today ! " + outputPath);
            }

        }

        if (true) {
            JobConf config = new JobConf(getConf(), getClass());
            config.set("user.args", Utils.asString(args));

            config.setJobName(getName() + "-" + System.currentTimeMillis());
            config.setNumReduceTasks(5);
            config.setMapperClass(AreaMapper.class);
            config.setReducerClass(LongSumReducer.class);
            config.setCombinerClass(LongSumReducer.class);
            config.setInputFormat(inputFormat);
            config.setMapOutputKeyClass(Text.class);
            config.setMapOutputValueClass(LongWritable.class);


            if (TextInputFormat.class.equals(inputFormat)) {
                TextInputFormat.addInputPath(config, new Path(outputTemp));
            } else if (SequenceFileInputFormat.class.equals(inputFormat)) {
                SequenceFileInputFormat.addInputPath(config, new Path(outputTemp));
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

    public static class AreaMapper implements Mapper<Object, Text, Text, LongWritable> {
        public void map(Object key, Text value, OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException {
            String[] _allCols = StringUtils.splitPreserveAllTokens(value.toString(), TAB);
            if (_allCols.length > 1) {
                if ("item".endsWith(_allCols[0])) {
                    output.collect(Utils.mergeKey("sum", "item_count"), ONE);
                    long count = NumberUtils.toLong(_allCols[2]);
                    output.collect(Utils.mergeKey("item", Sku.getArea(count)), ONE);

                } else if ("outer".equals(_allCols[0])) {
                    output.collect(Utils.mergeKey("sum", "outer_count"), ONE);
                    long count = NumberUtils.toLong(_allCols[2]);
                    output.collect(Utils.mergeKey("outer", Sku.getArea(count)), ONE);
                    String[] _array = StringUtils.splitPreserveAllTokens(_allCols[1], '|');
                    output.collect(Utils.mergeKey("seller", _array[0]), ONE);
                } else if ("item_all".equals(_allCols[0])) {
                    output.collect(Utils.mergeKey("sum", "item"), new LongWritable(NumberUtils.toLong(_allCols[1])));
                } else if ("outer_all".equals(_allCols[0])) {
                    output.collect(Utils.mergeKey("sum", "outer"), new LongWritable(NumberUtils.toLong(_allCols[1])));
                } else if ("sku_all".equals(_allCols[0])) {
                    output.collect(Utils.mergeKey("sum", "sku"), new LongWritable(NumberUtils.toLong(_allCols[1])));
                } else if ("sku_created".equals(_allCols[0])) {
                    output.collect(Utils.mergeKey("sum", "created"), new LongWritable(NumberUtils.toLong(_allCols[1])));
                } else if ("sku_modified".equals(_allCols[0])) {
                    output.collect(Utils.mergeKey("sum", "modified"), new LongWritable(NumberUtils.toLong(_allCols[1])));
                } else if ("sku_status".equals(_allCols[0])) {
                    output.collect(Utils.mergeKey("sum", "sku_status_" + _allCols[1]), new LongWritable(NumberUtils.toLong(_allCols[2])));
                }
            }
        }

        public void configure(JobConf job) {
        }

        public void close() throws IOException {
        }
    }

    @Override
    public void map(Object key, Text value, OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException {
        String[] _allCols = StringUtils.splitPreserveAllTokens(value.toString(), TAB);
        if (_allCols.length < 11) {
            return;
        }
        output.collect(Utils.mergeKey("sku_all"), ONE);
        output.collect(Utils.mergeKey("sku_status", _allCols[Sku.status]), ONE);

        String gmtCreated = _allCols[Sku.gmt_create];
        String gmtModified = _allCols[Sku.gmt_modified];
        String queryDate = DateStringUtils.format( this.inputArgs[0]);
        if (Utils.isSameDay(queryDate, gmtCreated)) {
            output.collect(Utils.mergeKey("sku_created"), ONE);
        }
        if (Utils.isSameDay(queryDate, gmtModified)) {
            output.collect(Utils.mergeKey("sku_modified"), ONE);
        }


        if (!"1".equals(_allCols[Sku.status])) {
            return;
        }
        output.collect(Utils.mergeKey("item", _allCols[Sku.item_id]), ONE);
        output.collect(Utils.mergeKey("item_all"), ONE);
        if (Utils.isNotNull(_allCols[Sku.outer_id])) {
            output.collect(Utils.mergeKey("outer", _allCols[Sku.seller_id] + "|" + _allCols[Sku.outer_id]), ONE);
            output.collect(Utils.mergeKey("outer_all"), ONE);
        }

    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new SkuCenter(),
                args);
        System.exit(res);
    }
}
