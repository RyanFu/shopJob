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
 * 角色性别百分比
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

					long sex = option.getLong("sex"); //性别

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
	 * JobConf：map/reduce的job配置类，向hadoop框架描述map-reduce执行的工作
	 * 构造方法：JobConf()、JobConf(Class exampleClass)、JobConf(Configuration
	 * conf)等
	 */
	JobConf conf = new JobConf(MysizeSex.class);
	conf.setJobName("womenPercent"); // 设置一个用户定义的job名称

	conf.setOutputKeyClass(Text.class); // 为job的输出数据设置Key类
	conf.setOutputValueClass(Text.class); // 为job输出设置value类

	conf.setMapperClass(Map.class); // 为job设置Mapper类
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
