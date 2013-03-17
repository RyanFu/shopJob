package org.dueam.hadoop.jobs;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;

/**
 * User: windonly
 * Date: 11-4-11 下午1:41
 */
public class LineCountFilter extends Configured implements Tool {
    public int run(String[] args) throws Exception {

        if (args.length < 3) {
            System.out.println("ERROR: Please Enter Input Path and length");
            return JobClient.SUCCESS;
        }

        JobConf conf = new JobConf(getConf(), LineCountFilter.class);
        conf.setJobName("LineCountFilter-" + System.currentTimeMillis());

        FileSystem fs = FileSystem.get(conf);
        String output = "/group/tbdev/xiaodu/suoni/line_count_filter";
        Path outputPath = new Path(output);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        conf.setNumReduceTasks(1);
        conf.setMapperClass(MapClass.class);
        conf.setReducerClass(LongSumReducer.class);
        conf.setInputFormat(TextInputFormat.class);
        if (args.length > 1 && "seq".equals(args[1])) {
            conf.setInputFormat(SequenceFileInputFormat.class);
        }

        conf.set("user.len", args[2]);

        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(LongWritable.class);
        if (args.length > 1 && "seq".equals(args[1])) {
            SequenceFileInputFormat.addInputPath(conf, new Path(args[0]));
        } else {
            TextInputFormat.addInputPath(conf, new Path(args[0]));
        }

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(LongWritable.class);

        FileOutputFormat.setOutputPath(conf, outputPath);

        JobClient.runJob(conf);

        return JobClient.SUCCESS;
    }


    public static class MapClass extends MapReduceBase implements
            Mapper<Object, Text, Text, LongWritable> {
        // 使用者 时间 preurl cururl
        public void map(Object key, Text value,
                        OutputCollector<Text, LongWritable> out, Reporter reporter)
                throws IOException {
            long recodeLen = value.toString().length();
            if(recodeLen > len){
                out.collect(value, new LongWritable(len));
            }
        }

        private int len;

		public void configure(JobConf job) {
			String tmp = job.get("user.len");
            this.len = NumberUtils.toInt(tmp,1000);
			System.out.println("configured : len = " + tmp);
		}
    }


    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new LineCountFilter(),
                args);
        System.exit(res);
    }
}
