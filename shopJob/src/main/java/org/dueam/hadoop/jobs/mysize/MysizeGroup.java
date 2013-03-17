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
 * 统计各个部位排列组合
 **/
public class MysizeGroup {
	public static class Map extends MapReduceBase implements
	Mapper<LongWritable, Text, Text, Text> {


		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String valueS = value.toString();
			// sg tz jk xw yw tw
			long sgtz = 0;
			long sgjk = 0;
			long sgxw = 0;
			long sgyw= 0;
			long sgtw = 0;
			long tzjk = 0;
			long tzxw= 0;
			long tzyw= 0;
			long tztw= 0;
			long jkxw = 0;
			long jkyw = 0;
			long jktw = 0;
			long xwyw = 0;
			long xwtw = 0;
			long twyw = 0;

			if(StringUtils.isNotBlank(valueS)){

				JSONArray sizeArray = JSONArray.fromObject(valueS);
				for (int i = 0, l = sizeArray.size(); i < l; i++) {
					JSONObject option = sizeArray.getJSONObject(i);

					long sg = option.getLong("sg"); //性别
					long tz = option.getLong("tz"); //性别
					long jk = option.getLong("jk"); //性别
					long xw = option.getLong("xw"); //性别
					long yw = option.getLong("yw"); //性别
					long tw = option.getLong("tw"); //性别
					if(sg*tz>0&&(jk==0&&xw==0&&yw==0&&tw==0)){
						sgtz++;
					}
					if(sg*jk>0&&(tz==0&&xw==0&&yw==0&&tw==0)){
						sgjk++;
					}
					if(sg*xw>0&&(tz==0&&jk==0&&yw==0&&tw==0)){
						sgxw++;
					}
					if(sg*yw>0&&(tz==0&&xw==0&&tw==0&&jk==0)){
						sgyw++;
					}
					if(sg*tw>0&&(tz==0&&xw==0&&yw==0&&jk==0)){
						sgtw++;
					}
					if(tz*jk>0&&(sg==0&&xw==0&&yw==0&&tw==0)){
						tzjk++;
					}
					if(tz*xw>0&&(sg==0&&yw==0&&tw==0&&jk==0)){
						tzxw++;
					}
					if(tz*yw>0&&(sg==0&&jk==0&&xw==0&&tw==0)){
						tzyw++;
					}
					if(tz*tw>0&&(sg==0&&jk==0&&xw==0&&yw==0)){
						tztw++;
					}
					if(jk*xw>0&&(sg==0&&tz==0&&tw==0&&yw==0)){
						jkxw++;
					}
					if(jk*yw>0&&(sg==0&&tz==0&&xw==0&&tw==0)){
						jkyw++;
					}
					if(jk*tw>0&&(sg==0&&tz==0&&xw==0&&yw==0)){
						jktw++;
					}
					if(xw*yw>0&&(sg==0&&jk==0&&tz==0&&tw==0)){
						xwyw++;
					}
					if(xw*tw>0&&(sg==0&&jk==0&&tz==0&&yw==0)){
						xwtw++;
					}
					if(yw*tw>0&&(sg==0&&jk==0&&xw==0&&tz==0)){
						twyw++;
					}

				}

				JSONObject jsonObject = new JSONObject();
				jsonObject.put("sgtz", sgtz);
				jsonObject.put("sgjk", sgjk);
				jsonObject.put("sgxw", sgxw);
				jsonObject.put("sgyw", sgyw);
				jsonObject.put("sgtw", sgtw);
				jsonObject.put("tzjk", tzjk);
				jsonObject.put("tzxw", tzxw);
				jsonObject.put("tzyw", tzyw);
				jsonObject.put("tztw", tztw);
				jsonObject.put("jkxw", jkxw);
				jsonObject.put("jkyw", jkyw);
				jsonObject.put("jktw", jktw);
				jsonObject.put("xwyw", xwyw);
				jsonObject.put("xwtw", xwtw);
				jsonObject.put("twyw", twyw);

				output.collect(new Text("goup"),new Text(jsonObject.toString()));

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
		long sgtz = 0;
		long sgjk = 0;
		long sgxw = 0;
		long sgyw= 0;
		long sgtw = 0;
		long tzjk = 0;
		long tzxw= 0;
		long tzyw= 0;
		long tztw= 0;
		long jkxw = 0;
		long jkyw = 0;
		long jktw = 0;
		long xwyw = 0;
		long xwtw = 0;
		long twyw = 0;

		while (values.hasNext()) {
			String value = values.next().toString();
			JSONObject jsonObject = JSONObject.fromObject(value);

			sgtz += jsonObject.getLong("sgtz");
			sgjk += jsonObject.getLong("sgjk");
			sgxw += jsonObject.getLong("sgxw");
			sgyw += jsonObject.getLong("sgyw");
			sgtw += jsonObject.getLong("sgtw");
			tzjk += jsonObject.getLong("tzjk");
			tzxw += jsonObject.getLong("tzxw");
			tzyw += jsonObject.getLong("tzyw");
			tztw += jsonObject.getLong("tztw");
			jkxw += jsonObject.getLong("jkxw");
			jkyw += jsonObject.getLong("jkyw");
			jktw += jsonObject.getLong("jktw");
			xwyw += jsonObject.getLong("xwyw");
			xwtw += jsonObject.getLong("xwtw");
			twyw += jsonObject.getLong("twyw");
		}

		output.collect(key, new Text(sgtz+"\t"+sgjk+"\t"+sgxw+"\t"+sgyw+"\t"+sgtw+"\t"+tzjk+"\t"+tzxw+"\t"+tzyw+"\t"+tztw+"\t"+jkxw+"\t"+jkyw+"\t"+jktw+"\t"+xwyw+"\t"+xwtw+"\t"+twyw));
	}
	}

	public static void main(String[] args) throws Exception {
	/**
	 * JobConf：map/reduce的job配置类，向hadoop框架描述map-reduce执行的工作
	 * 构造方法：JobConf()、JobConf(Class exampleClass)、JobConf(Configuration
	 * conf)等
	 */
	JobConf conf = new JobConf(MysizeGroup.class);
	conf.setJobName("group"); // 设置一个用户定义的job名称

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
