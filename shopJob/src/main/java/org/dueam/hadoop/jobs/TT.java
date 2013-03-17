package org.dueam.hadoop.jobs;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.dueam.hadoop.common.util.DateStringUtils;
import org.dueam.hadoop.common.util.Utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

/**
 * User: windonly
 * Date: 11-6-23 ÏÂÎç2:37
 */
public class TT extends Configured implements Tool {
    public int run(String[] args) throws Exception {
        String queryDate = args[0];
        String table = args[1];
        JobConf config = new JobConf(getConf(), getClass());
        config.set("user.args", Utils.asString(args));
        String input = "/group/tbads/dbsync/TimeTunnel2/" + table + "/" + DateStringUtils.format(queryDate);
        String output = "/group/tbbp/tt/" + table + "/" + queryDate;

        config.setJobName("TT - " + table + System.currentTimeMillis());
        config.setNumReduceTasks(100);
        config.setMapperClass(TTMap.class);
        config.setReducerClass(TTReduce.class);
        config.setInputFormat(SequenceFileInputFormat.class);
        config.setMapOutputKeyClass(LongWritable.class);
        config.setMapOutputValueClass(Text.class);

        //add input paths
        Path inputPath = new Path(input);
        FileSystem fs = FileSystem.get(config);
        List<Path> pathList = new ArrayList<Path>();
        listPath(pathList, fs, inputPath);
        for (Path path : pathList) {
            if (!path.getName().endsWith("tmp")) {
                SequenceFileInputFormat.addInputPath(config, path);

            }
        }

        config.setOutputKeyClass(NullWritable.class);
        config.setOutputValueClass(Text.class);

        Path outputPath = new Path(output);
        FileOutputFormat.setOutputPath(config, outputPath);

        if (!fs.exists(outputPath)) {
            JobClient.runJob(config);
        } else {
            System.out.println("You has finished this job today ! " + outputPath);
        }

        return JobClient.SUCCESS;
    }

    private static char CTRL_B = (char) 0x02;

    public static class TTMap extends MapReduceBase implements Mapper<Object, Text, LongWritable, Text> {

        public void map(Object key, Text value, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
            String[] _allCols = StringUtils.splitPreserveAllTokens(value.toString(), CTRL_B);

            if (_allCols.length > 0) {
                output.collect(new LongWritable(RandomUtils.nextLong()), new Text(_allCols[_allCols.length - 1]));
            }
        }
    }

    public static class  TTReduce extends MapReduceBase implements Reducer<LongWritable,Text,NullWritable,Text>{

        public void reduce(LongWritable key, Iterator<Text> values, OutputCollector<NullWritable, Text> output, Reporter reporter) throws IOException {
            while (values.hasNext()) {
                output.collect(NullWritable.get(),values.next());
            }
        }
    }

    public void listPath(List<Path> pathList, FileSystem fs, Path inputPath) throws IOException {
        if (fs.isFile(inputPath)) {
            pathList.add(inputPath);
        } else {
            FileStatus[] fileStatuses = fs.listStatus(inputPath);
            for (FileStatus fileStatus : fileStatuses) {
                listPath(pathList, fs, fileStatus.getPath());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),
                new TT(), args);
        System.exit(res);
    }
}
