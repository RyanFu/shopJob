/**
 * Ь��ģ�������ģ��������ų�/�ų��ͽſ�
 */
package org.dueam.hadoop.jobs.mysize.tp;

import java.io.IOException;
import java.util.Iterator;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

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
import org.dueam.hadoop.jobs.mysize.MysizeSex;

/**
 * @author buhan
 *
 */
public class XiePart {

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			String valueS = value.toString();
			long jak = 0;
			long sum = 0;

			if (StringUtils.isNotBlank(valueS)) {

				JSONArray sizeArray = JSONArray.fromObject(valueS);
				JSONObject option = sizeArray.getJSONObject(0);


				if (null!=option.get("jak")) {
					jak++;
				}
				sum++;

				JSONObject jsonObject = new JSONObject();
				jsonObject.put("jak", jak);
				jsonObject.put("sum", sum);

				output.collect(new Text("xiePart-"), new Text(jsonObject.toString()));

			} else {
				return;
			}
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			long jacSum = 0;
			long countSum = 0;

			while (values.hasNext()) {                                                                             
				String value = values.next().toString();
				JSONObject jsonObject = JSONObject.fromObject(value);
				jacSum += jsonObject.getLong("jak");
				countSum += jsonObject.getLong("sum");
			}

			output.collect(key, new Text(jacSum + "\t" + countSum));
		}
	}

	public static void main(String[] args) throws Exception {
		/**
		 * JobConf��map/reduce��job�����࣬��hadoop�������map-reduceִ�еĹ���
		 * ���췽����JobConf()��JobConf(Class exampleClass)��JobConf(Configuration
		 * conf)��
		 */
		JobConf conf = new JobConf(MysizeSex.class);
		conf.setJobName("jacPercent"); // ����һ���û������job����

		conf.setOutputKeyClass(Text.class); // Ϊjob�������������Key��
		conf.setOutputValueClass(Text.class); // Ϊjob�������value��

		conf.setMapperClass(Map.class); // Ϊjob����Mapper��
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
