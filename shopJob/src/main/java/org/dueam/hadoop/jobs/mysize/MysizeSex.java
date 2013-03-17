package org.dueam.hadoop.jobs.mysize;

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

/**
 * ��ɫ�Ա�ٷֱ�
 **/
public class MysizeSex {
	public static class Map extends MapReduceBase implements
	Mapper<LongWritable, Text, Text, Text> {


		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String valueS = value.toString();
			long women = 0;
			long sum = 0;

			if(StringUtils.isNotBlank(valueS)){

				JSONArray sizeArray = JSONArray.fromObject(valueS);
				for (int i = 0, l = sizeArray.size(); i < l; i++) {
					JSONObject option = sizeArray.getJSONObject(i);

					long sex = option.getLong("sex"); //�Ա�

					if(sex==2){
						women++;
					}
					sum++;

				}
				JSONObject jsonObject = new JSONObject();
				jsonObject.put("women", women);
				jsonObject.put("sum", sum);

				output.collect(new Text("sex"),new Text(jsonObject.toString()));

			}else{
				return;
			}
		}
	}

	public static class Reduce extends MapReduceBase implements
		Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		long sexSum = 0;
		long countSum =0;

		while (values.hasNext()) {
			String value = values.next().toString();
			JSONObject jsonObject = JSONObject.fromObject(value);
			sexSum += jsonObject.getLong("women");
			countSum += jsonObject.getLong("sum");
		}

		output.collect(key, new Text(sexSum+"\t"+countSum));
	}
	}

	public static void main(String[] args) throws Exception {
	/**
	 * JobConf��map/reduce��job�����࣬��hadoop�������map-reduceִ�еĹ���
	 * ���췽����JobConf()��JobConf(Class exampleClass)��JobConf(Configuration
	 * conf)��
	 */
	JobConf conf = new JobConf(MysizeSex.class);
	conf.setJobName("womenPercent"); // ����һ���û������job����

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
