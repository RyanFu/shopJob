package org.dueam.hadoop.jobs.mysize;

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
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

/**
 * 统计热点部位排名
 **/
public class ClickPart {
	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		static String[] partName = new String[] { "jac", "jak", "sxw", "xxw",
				"xw", "jk", "yw", "tw", "bc", "tc", "dtw", "sg", "tz" };

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String valueS = value.toString();
			long jacCount = 0;
			long jakCount = 0;
			long sxwCount = 0;
			long xxwCount = 0;
			long xwCount = 0;
			long jkCount = 0;
			long ywCount = 0;
			long twCount = 0;
			long bcCount = 0;
			long tcCount = 0;
			long dtwCount = 0;
			long sgCount = 0;
			long tzCount = 0;

			if (StringUtils.isNotBlank(valueS)) {

				JSONArray sizeArray = JSONArray.fromObject(valueS);
				for (int i = 0, l = sizeArray.size(); i < l; i++) {
					JSONObject option = sizeArray.getJSONObject(i);

					long json_jac = option.getLong("jac"); // 脚长
					long json_jak = option.getLong("jak"); // 脚宽

					long json_sxw = option.getLong("sxw"); // 上胸围
					long json_xxw = option.getLong("xxw"); // 下胸围

					long json_xw = option.getLong("xw"); // 肩宽
					long json_jk = option.getLong("jk"); // 肩宽
					long json_yw = option.getLong("yw"); // 腰围
					long json_tw = option.getLong("tw"); // 臀围
					long json_bc = option.getLong("bc"); // 臂长
					long json_tc = option.getLong("tc"); // 腿长
					long json_dtw = option.getLong("dtw"); // 大腿围
					long json_sg = option.getLong("sg"); // 身高
					long json_tz = option.getLong("tz"); // 体重

					if (json_jac != 0) {
						jacCount++;
					}
					if (json_jak != 0) {
						jakCount++;
					}
					if (json_sxw != 0) {
						sxwCount++;
					}
					if (json_xxw != 0) {
						xxwCount++;
					}
					if (json_xw != 0) {
						xwCount++;
					}
					if (json_jk != 0) {
						jkCount++;
					}
					if (json_yw != 0) {
						ywCount++;
					}
					if (json_tw != 0) {
						twCount++;
					}
					if (json_bc != 0) {
						bcCount++;
					}
					if (json_tc != 0) {
						tcCount++;
					}
					if (json_dtw != 0) {
						dtwCount++;
					}
					if (json_sg != 0) {
						sgCount++;
					}
					if (json_tz != 0) {
						tzCount++;
					}
				}
				JSONObject jsonObject = new JSONObject();
				jsonObject.put("jac", jacCount);
				jsonObject.put("jak", jakCount);
				jsonObject.put("sxw", sxwCount);
				jsonObject.put("xxw", xxwCount);
				jsonObject.put("xw", xwCount);
				jsonObject.put("yw", ywCount);
				jsonObject.put("tw", twCount);
				jsonObject.put("bc", bcCount);
				jsonObject.put("jk", jkCount);
				jsonObject.put("tc", tcCount);
				jsonObject.put("sg", sgCount);
				jsonObject.put("tz", tzCount);
				jsonObject.put("dtw", dtwCount);

				output.collect(new Text("part"),
						new Text(jsonObject.toString()));

			} else {
				return;
			}
		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			long jacSum = 0;
			long jakSum = 0;
			long sxwSum = 0;
			long xxwSum = 0;
			long xwSum = 0;
			long ywSum = 0;
			long twSum = 0;
			long bcSum = 0;
			long jkSum = 0;
			long tcSum = 0;
			long sgSum = 0;
			long tzSum = 0;
			long dtwSum = 0;

			while (values.hasNext()) {
				String value = values.next().toString();
				JSONObject jsonObject = JSONObject.fromObject(value);
				jacSum += jsonObject.getLong("jac");
				jakSum += jsonObject.getLong("jak");
				sxwSum += jsonObject.getLong("sxw");
				xxwSum += jsonObject.getLong("xxw");
				xwSum += jsonObject.getLong("xw");
				ywSum += jsonObject.getLong("yw");
				twSum += jsonObject.getLong("tw");
				bcSum += jsonObject.getLong("bc");
				jkSum += jsonObject.getLong("jk");
				tcSum += jsonObject.getLong("tc");
				sgSum += jsonObject.getLong("sg");
				tzSum += jsonObject.getLong("tz");
				dtwSum += jsonObject.getLong("dtw");
			}

			JSONObject jsonObject1 = new JSONObject();
			jsonObject1.put("jac", jacSum);
			jsonObject1.put("jak", jakSum);
			jsonObject1.put("sxw", sxwSum);
			jsonObject1.put("xxw", xxwSum);
			jsonObject1.put("xw", xwSum);
			jsonObject1.put("yw", ywSum);
			jsonObject1.put("tw", twSum);
			jsonObject1.put("bc", bcSum);
			jsonObject1.put("jk", jkSum);
			jsonObject1.put("tc", tcSum);
			jsonObject1.put("sg", sgSum);
			jsonObject1.put("tz", tzSum);
			jsonObject1.put("dtw", dtwSum);

			output.collect(key, new Text(jsonObject1.toString()));
		}
	}

	public static void main(String[] args) throws Exception {
		/**
		 * JobConf：map/reduce的job配置类，向hadoop框架描述map-reduce执行的工作
		 * 构造方法：JobConf()、JobConf(Class exampleClass)、JobConf(Configuration
		 * conf)等
		 */
		JobConf conf = new JobConf(ClickPart.class);
		conf.setJobName("ClickPart"); // 设置一个用户定义的job名称

		conf.setOutputKeyClass(Text.class); // 为job的输出数据设置Key类
		conf.setOutputValueClass(Text.class); // 为job输出设置value类

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
