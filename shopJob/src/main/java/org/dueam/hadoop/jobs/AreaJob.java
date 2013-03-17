package org.dueam.hadoop.jobs;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.dueam.hadoop.common.HadoopTable;
import org.dueam.hadoop.common.MapCallback;
import org.dueam.hadoop.common.util.Utils;

import java.io.IOException;

/**
 * 区间统计JOB
 * User: windonly
 * Date: 11-4-7 上午9:36
 */
public abstract class AreaJob extends Configured implements Tool,Mapper<Object, Text, Text, LongWritable> {
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Miss args : inputPath outputPath");
            return JobClient.SUCCESS;
        }
        String input = args[0];
        String output = args[1];
        String type = "TextInputFormat";
        if (args.length > 2) {
            type = args[2];
        }

        if (true) {
            JobConf config = new JobConf(getConf(), getClass());
            config.set("user.args", Utils.asString(args));

            config.setJobName(getClass().getSimpleName() + "-" + System.currentTimeMillis());
            config.setNumReduceTasks(1);
            config.setReducerClass(LongSumReducer.class);
            config.setCombinerClass(LongSumReducer.class);
            config.setMapperClass(getClass());

            config.setMapOutputKeyClass(Text.class);
            config.setMapOutputValueClass(LongWritable.class);

            Class inputFormat = HadoopTable.inputFormat(type);
            if (TextInputFormat.class.equals(inputFormat)) {
                config.setInputFormat(TextInputFormat.class);
                TextInputFormat.addInputPath(config, new Path(input));
            } else if (SequenceFileInputFormat.class.equals(inputFormat)) {
                config.setInputFormat(SequenceFileInputFormat.class);
                SequenceFileInputFormat.addInputPath(config, new Path(input));
            }
            //add input paths

            config.setOutputKeyClass(Text.class);
            config.setOutputValueClass(LongWritable.class);

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


    public final static LongWritable ONE = new LongWritable(1);

    public void map(Object key, Text value, OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException {
          mapCallback(value, output);
    }

    abstract void mapCallback(Text value,OutputCollector<Text, LongWritable> output) throws IOException;

    public void close() throws IOException {
    }

    public void configure(JobConf job) {
    }





}
