package org.dueam.hadoop.jobs;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.dueam.hadoop.common.util.DateStringUtils;
import org.dueam.hadoop.common.util.Utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * User: windonly
 * Date: 11-1-4 ÏÂÎç1:14
 */
public class UserViewMuliHostStepOneFilter extends Configured implements Tool, Reducer<Text, Text, Text, Text>, Mapper<Object, Text, Text, Text> {

    public int run(String[] args) throws Exception {
        if (args.length < 1) {
            args = new String[]{DateStringUtils.now()};
            System.out.println("ERROR: Please Enter Date , eg. 20101010 ! now use default => " + DateStringUtils.now());
        }

        JobConf config = new JobConf(getConf(), getClass());
        config.set("user.args", Utils.asString(args));

        config.setJobName(getClass() + "-" + System.currentTimeMillis());
        config.setNumReduceTasks(100);
        config.setMapperClass(getClass());
        config.setReducerClass(getClass());
        config.setInputFormat(getInputFormat());
        config.setMapOutputKeyClass(Text.class);
        config.setMapOutputValueClass(Text.class);

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

        if (!fs.exists(outputPath)) {
            JobClient.runJob(config);
        } else {
            System.out.println("You has finished this job today ! " + outputPath);
        }

        return JobClient.SUCCESS;
    }

    protected String[] getInputPath(String[] args) {
        //String[] input = new String[]{HadoopTable.atpanel(args[0])};

        String[] input = new String[]{"/group/tbdev/xiaodu/suoni/user_view_muli_host_setp_one/" + args[0]};
        if (args.length > 1 && "false".equals(args[1])) {
            input = new String[]{"/group/tbdev/xiaodu/suoni/user_view_muli_host_setp_one_all/" + args[0]};
        }
        System.out.println("input path => " + Utils.asString(input));
        return input;
    }

    protected Class getInputFormat() {
        return TextInputFormat.class;
    }

    protected String getOutputPath(String[] args) {
        if (args.length > 1 && "false".equals(args[1])) {
            return "/group/tbdev/xiaodu/suoni/user_view_muli_host_setp_one_filter_all/" + args[0];
        }
        return "/group/tbdev/xiaodu/suoni/user_view_muli_host_setp_one_filter/" + args[0];
    }

    public void map(Object key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        String[] _allCols = StringUtils.splitPreserveAllTokens(value.toString(), TAB);
        output.collect(Utils.mergeKey(_allCols[0],_allCols[1].substring(0,12)+"00",_allCols[2],_allCols[3]), value);
    }

    protected String[] inputArgs;

    public void configure(JobConf job) {
        this.inputArgs = Utils.asArray(job.get("user.args"));
    }

    final static String DATETIME_STYLE = "yyyyMMddhhmmss";

    public static boolean isSameTime(String begin, String end) {
        long s = Math.abs(DateStringUtils.second(begin, end, DATETIME_STYLE));
        return s <= 1800;
    }

    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        String[] tokens = StringUtils.splitPreserveAllTokens(values.next().toString(), TAB);
        output.collect(new Text(tokens[0]),Utils.mergeKey(tokens[1],tokens[2],tokens[3],tokens[4]));


    }

    final static char TAB = (char) 0x09;


    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),
                new UserViewMuliHostStepOneFilter(), args);
        System.exit(res);
    }


    public void close() throws IOException {

    }


}
