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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;

/**
 * User: windonly
 * Date: 11-4-11 下午1:41
 */
public class LineCount extends Configured implements Tool {
    public int run(String[] args) throws Exception {

        if (args.length < 1) {
            System.out.println("ERROR: Please Enter Input Path");
            return JobClient.SUCCESS;
        }

        JobConf conf = new JobConf(getConf(), LineCount.class);
        conf.setJobName("LineCount-" + System.currentTimeMillis());

        FileSystem fs = FileSystem.get(conf);
        String output = "/group/tbdev/xiaodu/suoni/line_count";
        Path outputPath = new Path(output);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        conf.setNumReduceTasks(1);
        conf.setMapperClass(MapClass.class);
        conf.setReducerClass(ReducerClass.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setCombinerClass(CReducerClass.class);
        if (args.length > 1 && "seq".equals(args[1])) {
            conf.setInputFormat(SequenceFileInputFormat.class);
        }

        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(Text.class);
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
            Mapper<Object, Text, Text, Text> {
        // 使用者 时间 preurl cururl
        public void map(Object key, Text value,
                        OutputCollector<Text, Text> out, Reporter reporter)
                throws IOException {
            long len = value.toString().length();
            out.collect(new Text("LEN"), new Text(len+"\t"+"1"+"\t"+len+"\t"+len));
        }
    }
    public static class CReducerClass extends MapReduceBase implements
            Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            long sum = 0, max = 0, min = Long.MAX_VALUE;
            long count = 0;
            while (values.hasNext()) {
                String[] array = StringUtils.splitPreserveAllTokens(values.next().toString(),(char)0x09);
                long value = NumberUtils.toLong(array[0]);
                count = count + NumberUtils.toLong(array[1]);
                sum = sum + value;
                if (NumberUtils.toLong(array[2]) > max) {
                    max = NumberUtils.toLong(array[2]);
                }
                if (NumberUtils.toLong(array[3]) < min) {
                    min = NumberUtils.toLong(array[3]);
                }
            }

            output.collect(key, new Text(sum +"\t"+count+"\t"+max+"\t"+min));

        }
    }
    public static class ReducerClass extends MapReduceBase implements
            Reducer<Text, Text, Text, LongWritable> {
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException {
            long sum = 0, max = 0, min = Long.MAX_VALUE;
            long count = 0;
            while (values.hasNext()) {
                String[] array = StringUtils.splitPreserveAllTokens(values.next().toString(),(char)0x09);
                long value = NumberUtils.toLong(array[0]);
                count = count + NumberUtils.toLong(array[1]);
                sum = sum + value;
                if (NumberUtils.toLong(array[2]) > max) {
                    max = NumberUtils.toLong(array[2]);
                }
                if (NumberUtils.toLong(array[3]) < min) {
                    min = NumberUtils.toLong(array[3]);
                }
            }

            output.collect(new Text("SUM"), new LongWritable(sum));
            output.collect(new Text("COUNT"), new LongWritable(count));
            output.collect(new Text("AVG"), new LongWritable(sum / count));
            output.collect(new Text("MAX"), new LongWritable(max));
            output.collect(new Text("MIN"), new LongWritable(min));

        }
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new LineCount(),
                args);
        System.exit(res);
    }
}
