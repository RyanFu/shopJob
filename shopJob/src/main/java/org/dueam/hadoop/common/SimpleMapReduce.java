package org.dueam.hadoop.common;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.dueam.hadoop.common.util.DateStringUtils;
import org.dueam.hadoop.common.util.Utils;

import java.io.IOException;
import java.util.Iterator;

/**
 * a simple map/reduce for taobao
 * User: windonly
 * Date: 10-12-10 ÏÂÎç4:07
 */
public abstract class SimpleMapReduce extends Configured implements Tool, Reducer<Text, LongWritable, Text, Text>, Mapper<Object, Text, Text, LongWritable> {
    protected String getName() {
        return getClass().getSimpleName();
    }

    protected final static LongWritable ONE = new LongWritable(1);

    protected abstract String[] getInputPath(String[] args);

    protected abstract Class getInputFormat();


    protected abstract String getOutputPath(String[] args);


    protected int taskNum = 1;

    protected void setup(String[] args){};

    public int run(String[] args) throws Exception {
        if (args.length < 1) {
            args = new String[]{DateStringUtils.now()};
            System.out.println("ERROR: Please Enter Date , eg. 20101010 ! now use default => " + DateStringUtils.now());
        }
        setup(args);

        JobConf config = new JobConf(getConf(), getClass());
        config.set("user.args", Utils.asString(args));

        config.setJobName(getName() + "-" + System.currentTimeMillis());
        config.setNumReduceTasks(taskNum);
        config.setMapperClass(getClass());
        config.setReducerClass(getClass());
        config.setInputFormat(getInputFormat());
        config.setMapOutputKeyClass(Text.class);
        config.setMapOutputValueClass(LongWritable.class);

        //add input paths
        for (String path : getInputPath(args)) {
            if (TextInputFormat.class.equals(getInputFormat())) {
                TextInputFormat.addInputPath(config, new Path(path));
            } else if (SequenceFileInputFormat.class.equals(getInputFormat())) {
                SequenceFileInputFormat.addInputPath(config, new Path(path));
            }
        }

        config.setOutputKeyClass(Text.class);
        config.setOutputValueClass(Text.class);

        //if output path exists then return
        FileSystem fs = FileSystem.get(config);
        Path outputPath = new Path(getOutputPath(args));
        FileOutputFormat.setOutputPath(config, outputPath);

        beforeRun(args,config);

        if (!fs.exists(outputPath)) {
            JobClient.runJob(config);
        } else {
            System.out.println("You has finished this job today ! " + outputPath);
        }

        return JobClient.SUCCESS;
    }

    protected void beforeRun(String[] args,JobConf config ){};
    protected static char TAB = (char) 0x09;
    protected static char CTRL_A = (char) 0x01;

    public void reduce(Text key, Iterator<LongWritable> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        long sum = 0, count = 0;
        while (values.hasNext()) {
            sum += values.next().get();
            count++;
        }
        StringBuffer _tmp = new StringBuffer();
        _tmp.append(sum).append(TAB).append(count);
        output.collect(key, new Text(_tmp.toString()));
    }

    public void map(Object key, Text value, OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException {
        doWork(value.toString(), output);
    }

    protected abstract void doWork(String line, OutputCollector<Text, LongWritable> output) throws IOException;

    protected String[] inputArgs;

    public void configure(JobConf job) {
        this.inputArgs = Utils.asArray(job.get("user.args"));
    }

    public void close() throws IOException {

    }
}
