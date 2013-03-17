/**
 *�����õķ�װģ������岿λ�ĸ������ͳ�ƣ�ֻչʾ����50��
 */
package org.dueam.hadoop.jobs.mysize.tp;

import java.io.IOException;
import java.util.Iterator;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

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
import org.dueam.hadoop.jobs.mysize.Mysize;

/**
 * @author buhan
 *
 */
public class TpGroup {

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, LongWritable> {

		private final static LongWritable one = new LongWritable(1);
		static String[] partName = new String[] { "jac", "jak", "sxw", "xxw", "xw", "jk", "yw", "tw", "bc", "tc", "dtw", "sg", "tz" };

		public void map(LongWritable key, Text value, OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException {
			String valueS = value.toString();
			if (StringUtils.isNotBlank(valueS)) {
				JSONArray sizeArray = JSONArray.fromObject(valueS);
				/**
				 * ���/����/���/��Χ/��Χ/��Χһ�ε�key��1/2/3/4/5/6 ���һ�������key,������ߺ����ص���ϣ�12
				 */
				String groupKey = getGroupKey(sizeArray);
				output.collect(new Text("tpGroup-"+groupKey), one);
			} else {
				return;
			}
		}
	}

	/**
	 * ���/����/���/��Χ/��Χ/��Χһ�ε�key��1/2/3/4/5/6 ���һ�������key,������ߺ����ص���ϣ�12
	 */
	public static String getGroupKey(JSONArray sizeArray) {

		StringBuffer sb = new StringBuffer();
		if(sizeArray.size()==0){
			return "0";
		}
		JSONObject option = (JSONObject) sizeArray.get(0);
		if (null != option.get("sg")) {
			sb.append("1");
		}
		if (null != option.get("tz")) {
			sb.append("2");
		}
		if (null != option.get("jk")) {
			sb.append("3");
		}
		if (null != option.get("xw")) {
			sb.append("4");
		}
		if (null != option.get("yw")) {
			sb.append("5");
		}
		if (null != option.get("tw")) {
			sb.append("6");
		}

		if(sb.length()==0){
			return "0";
		}
		return sb.toString();
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
		 * JobConf��map/reduce��job�����࣬��hadoop�������map-reduceִ�еĹ���
		 * ���췽����JobConf()��JobConf(Class exampleClass)��JobConf(Configuration
		 * conf)��
		 */
		JobConf conf = new JobConf(Mysize.class);
		conf.setJobName("Mysize_TP"); // ����һ���û������job����

		conf.setOutputKeyClass(Text.class); // Ϊjob�������������Key��
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
