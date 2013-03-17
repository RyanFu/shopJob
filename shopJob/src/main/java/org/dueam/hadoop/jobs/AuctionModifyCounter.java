package org.dueam.hadoop.jobs;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.dueam.hadoop.common.HadoopTable;
import org.dueam.hadoop.common.tables.Atpanel;
import org.dueam.hadoop.common.tables.AuctionAuctions;
import org.dueam.hadoop.common.util.DateStringUtils;
import org.dueam.hadoop.common.util.Utils;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * 域名同时在线链接（URL）
 * User: windonly
 * Date: 11-1-4 下午1:14
 */
public class AuctionModifyCounter extends Configured implements Tool, Mapper<Object, Text, Text, LongWritable> {
    public int run(String[] args) throws Exception {
        if (args.length < 1) {
            //args = new String[]{DateStringUtils.now()};
            System.out.println("ERROR: Please Enter Date , eg. 20101010 ! now use default => " + DateStringUtils.now());
            return JobClient.SUCCESS;
        }

        JobConf config = new JobConf(getConf(), getClass());
        config.set("user.args", Utils.asString(args));

        config.setJobName(getClass() + "-" + System.currentTimeMillis());
        config.setNumReduceTasks(25);
        config.setMapperClass(getClass());
        config.setReducerClass(LongSumReducer.class);
        config.setInputFormat(getInputFormat());
        config.setMapOutputKeyClass(Text.class);
        config.setMapOutputValueClass(LongWritable.class);
        config.setCombinerClass(LongSumReducer.class);

        //add input paths
        for (String path : getInputPath(args)) {
            if (TextInputFormat.class.equals(getInputFormat())) {
                TextInputFormat.addInputPath(config, new Path(path));
            } else if (SequenceFileInputFormat.class.equals(getInputFormat())) {
                Path inputPath = new Path(path);
                FileSystem fs = FileSystem.get(config);
                if (fs.isFile(inputPath)) {
                    SequenceFileInputFormat.addInputPath(config, inputPath);
                } else {
                    FileStatus[] fileStatuses = fs.listStatus(inputPath);
                    for (FileStatus fileStatus : fileStatuses) {
                        if (!fileStatus.isDir()) {
                            Path tmp = fileStatus.getPath();
                            if (!tmp.getName().endsWith("tmp")) {
                                SequenceFileInputFormat.addInputPath(config, tmp);
                            }
                        }
                    }
                }


            }
        }

        config.setOutputKeyClass(Text.class);
        config.setOutputValueClass(LongWritable.class);

        //if output path exists then return
        FileSystem fs = FileSystem.get(config);
        Path outputPath = new Path(getOutputPath(args));
        FileOutputFormat.setOutputPath(config, outputPath);

        if (!fs.exists(outputPath)) {
            JobClient.runJob(config);
        } else {
            System.out.println("You has finished this job today ! " + outputPath);
        }

        return JobClient.SUCCESS;
    }

    protected String[] getInputPath(String[] args) {
        String[] input = new String[]{"/group/tbads/dbsync/TimeTunnel2/AUCTION_AUCTIONS/" + args[0] + "/gate48.cm3"};
        System.out.println("input path => " + Utils.asString(input));
        return input;
    }

    protected Class getInputFormat() {
        return SequenceFileInputFormat.class;
    }

    protected String getOutputPath(String[] args) {
        return "/group/tbdev/xiaodu/suoni/auction_modify_counter/" + args[0];
    }

    public void map(Object key, Text value, OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException {
        String[] fields = StringUtils.splitPreserveAllTokens(value.toString(), CTRL_B);
        String[] _allCols = StringUtils.splitPreserveAllTokens(fields[4], CTRL_A);
        if (_allCols.length < 10) return;
        String modify = _allCols[AuctionAuctions.GMT_MODIFIED];
        output.collect(new Text(modify.substring(0, 16)), new LongWritable(1));
    }

    protected String[] inputArgs;

    public void configure(JobConf job) {
        this.inputArgs = Utils.asArray(job.get("user.args"));
    }


    final static char CTRL_A = 0x01;
    final static char CTRL_B = 0x02;
    final static char TAB = 0x09;


    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),
                new AuctionModifyCounter(), args);
        System.exit(res);
    }


    public void close() throws IOException {

    }
}
