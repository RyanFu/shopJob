package org.dueam.hadoop.jobs;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Map;

/**
 * User: windonly
 * Date: 11-6-14 ÏÂÎç5:21
 */
public class FileTest extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),
                new FileTest(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        if (args.length < 4) {
            System.out.println("ERROR: Please Enter args : input output type(text|seq) splitChar(9=\t)");
            return JobClient.SUCCESS ;
        }
        String input = args[0];
        String output = args[1];
        String type = args[2];
        String splitChar = args[3];

        JobConf config = new JobConf(getConf(), getClass());
        config.set("user.split", splitChar);

        config.setJobName("File Filter -" + System.currentTimeMillis());
        config.setNumReduceTasks(10);
        config.setReducerClass(IdentityReducer.class);
        config.setMapperClass(FileTestMapper.class);
        if ("text".equals(type)) {
            config.setInputFormat(TextInputFormat.class);
            TextInputFormat.addInputPath(config, new Path(input));
        } else {
            config.setInputFormat(SequenceFileInputFormat.class);
            SequenceFileInputFormat.addInputPath(config, new Path(input));
        }
        config.setMapOutputKeyClass(Text.class);
        config.setMapOutputValueClass(Text.class);


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

        return JobClient.SUCCESS;
    }


    public static class FileTestMapper extends MapReduceBase implements Mapper<Object, Text, Text, Text> {
        @Override
        public void configure(JobConf config) {
            splitChar = (char) NumberUtils.toInt(config.get("user.split"));

        }

        private char splitChar;

        public void map(Object key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            String[] _allCols = StringUtils.splitPreserveAllTokens(value.toString(), splitChar);
            StringBuffer stringBuffer = new StringBuffer();

            for (int i = 0; i < _allCols.length; i++) {
                stringBuffer.append(i).append('=').append(_allCols[i]).append("\t");
            }
            output.collect(new Text(_allCols[0]), new Text(stringBuffer.toString()));
        }
    }

}
