/**
 *单个卖家启用的模板个数分组
 */
package org.dueam.hadoop.jobs.mysize.tp;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.dueam.hadoop.jobs.mysize.Mysize;

/**
 * @author buhan
 *
 */
public class CountTp {

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, LongWritable> {

		private final static LongWritable one = new LongWritable(1);

		public void map(LongWritable key, Text value, OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException {
			String valueS = value.toString();
			if (StringUtils.isNotBlank(valueS)) {
				output.collect(new Text("countTp-"+valueS), one);
			} else {
				return;
			}
		}
	}


	public static class Reduce extends MapReduceBase implements Reducer<Text, LongWritable, Text, LongWritable> {

		public void reduce(Text key, Iterator<LongWritable> values, OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException {
			long sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new LongWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		/**
		 * JobConf：map/reduce的job配置类，向hadoop框架描述map-reduce执行的工作
		 * 构造方法：JobConf()、JobConf(Class exampleClass)、JobConf(Configuration
		 * conf)等
		 */
		JobConf conf = new JobConf(Mysize.class);
		conf.setJobName("Mysize_TP"); // 设置一个用户定义的job名称

		conf.setOutputKeyClass(Text.class); // 为job的输出数据设置Key类
		conf.setOutputValueClass(LongWritable.class); // 为job输出设置value类

		conf.setMapperClass(Map.class); // 为job设置Mapper类
		conf.setCombinerClass(Reduce.class); // 为job设置Combiner类
		conf.setReducerClass(Reduce.class); // 为job设置Reduce类

		conf.setInputFormat(TextInputFormat.class); // 为map-reduce任务设置InputFormat实现类
		conf.setOutputFormat(TextOutputFormat.class); // 为map-reduce任务设置OutputFormat实现类

		/**
		 * InputFormat描述map-reduce中对job的输入定义 setInputPaths():为map-reduce
		 * job设置路径数组作为输入列表 setInputPath()：为map-reduce job设置路径数组作为输出列表
		 */
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf); // 运行一个job
	}

}
