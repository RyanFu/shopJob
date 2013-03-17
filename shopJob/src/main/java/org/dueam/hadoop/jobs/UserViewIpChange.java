package org.dueam.hadoop.jobs;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.dueam.hadoop.common.HadoopTable;
import org.dueam.hadoop.common.tables.Atpanel;
import org.dueam.hadoop.common.util.DateStringUtils;
import org.dueam.hadoop.common.util.Utils;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * 查看当前用户访问淘宝的过程中是否存在IP变更情况
 * 模型：如果10分钟内访问IP发生变化的用户
 * User: windonly
 * Date: 11-1-4 下午1:14
 */
public class UserViewIpChange extends Configured implements Tool, Reducer<Text, Text, Text, Text>, Mapper<Object, Text, Text, Text> {

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
        String[] input = new String[]{HadoopTable.atpanel(args[0])};
        System.out.println("input path => " + Utils.asString(input));
        return input;
    }

    protected Class getInputFormat() {
        return SequenceFileInputFormat.class;
    }

    protected String getOutputPath(String[] args) {
        return "/group/tbdev/xiaodu/suoni/user_view_ip_change/" + args[0];
    }

    public void map(Object key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        doWork(value.toString(), output);
    }

    protected String[] inputArgs;

    public void configure(JobConf job) {
        this.inputArgs = Utils.asArray(job.get("user.args"));
    }
    private final static char TAB = 0x09;
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        Set<String> ipSet = new HashSet<String>();
        while (values.hasNext()) {
            String ip = values.next().toString();
            //获取IP段
            //ip = StringUtils.substring(ip,0,ip.lastIndexOf('.'));
            if(!ipSet.contains(ip)){
                ipSet.add(ip) ;
                if(ipSet.size() > 3){
                    break;
                }
            }
        }
        if (ipSet.size() > 1) {
            output.collect(key,new Text(StringUtils.join(ipSet,'|')));
        }
    }

    protected void doWork(String line, OutputCollector<Text, Text> output) throws IOException {
        String[] _allCols = StringUtils.splitPreserveAllTokens(line, '"');
        if (_allCols.length < 8) return;
        String mid = _allCols[Atpanel.mid];
        String time = _allCols[Atpanel.time];
        String ip = _allCols[Atpanel.ip];
        String uid = _allCols[Atpanel.uid];
        //过滤非会员访问
        if(NumberUtils.toLong(uid,0) <=  0){
            return;
        }
        if (StringUtils.isNotEmpty(mid) && StringUtils.isNotEmpty(ip) && StringUtils.isNotEmpty(time)) {
            output.collect(Utils.mergeKey(mid,StringUtils.substring(time,0,11)), Utils.mergeKey(ip));
        }


    }


    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),
                new UserViewIpChange(), args);
        System.exit(res);
    }


    public void close() throws IOException {

    }
}
