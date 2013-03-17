package org.dueam.hadoop.jobs.mysize;

import java.io.IOException;

import java.util.Iterator;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
import net.sf.json.JSONArray;

/**
 * ͳ���û�������ɫ����
 **/
public class Mysize {
	public static class Map extends MapReduceBase implements
	Mapper<LongWritable, Text, IntWritable, LongWritable> {

	private final static LongWritable one = new LongWritable(1);

		public void map(LongWritable key, Text value,
				OutputCollector<IntWritable, LongWritable> output, Reporter reporter)
				throws IOException {
			String valueS = value.toString();
			if(StringUtils.isNotBlank(valueS)){
				JSONArray sizeArray = JSONArray.fromObject(valueS);
				int roleIdNum = sizeArray.size();
				IntWritable a = new IntWritable(roleIdNum);
				output.collect(a,one);
			}else{
				return;
			}
		}
	}

	public static class Reduce extends MapReduceBase implements
		Reducer<IntWritable, LongWritable, IntWritable, LongWritable> {

	public void reduce(IntWritable key, Iterator<LongWritable> values,
			OutputCollector<IntWritable, LongWritable> output, Reporter reporter)
			throws IOException {
		long sum = 0;
		while (values.hasNext()) {
			sum += values.next().get();
		}
		output.collect(key, new LongWritable(sum));
	}
	}

	public static void main(String[] args) throws Exception {
	/**
	 * JobConf��map/reduce��job�����࣬��hadoop�������map-reduceִ�еĹ���
	 * ���췽����JobConf()��JobConf(Class exampleClass)��JobConf(Configuration
	 * conf)��
	 */
	JobConf conf = new JobConf(Mysize.class);
	conf.setJobName("Mysize"); // ����һ���û������job����

	conf.setOutputKeyClass(IntWritable.class); // Ϊjob�������������Key��
	conf.setOutputValueClass(LongWritable.class); // Ϊjob�������value��

	conf.setMapperClass(Map.class); // Ϊjob����Mapper��
	conf.setCombinerClass(Reduce.class); // Ϊjob����Combiner��
	conf.setReducerClass(Reduce.class); // Ϊjob����Reduce��

	conf.setInputFormat(TextInputFormat.class); // Ϊmap-reduce��������InputFormatʵ����
	conf.setOutputFormat(TextOutputFormat.class); // Ϊmap-reduce��������OutputFormatʵ����

	/**
	 * InputFormat����map-reduce�ж�job�����붨�� setInputPaths():Ϊmap-reduce
	 * job����·��������Ϊ�����б� setInputPath()��Ϊmap-reduce job����·��������Ϊ����б�
	 */
	FileInputFormat.setInputPaths(conf, new Path(args[0]));
	FileOutputFormat.setOutputPath(conf, new Path(args[1]));

	JobClient.runJob(conf); // ����һ��job
	}
	}
