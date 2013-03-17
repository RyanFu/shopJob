/**
 *���Ʊ�����Ŀ����Ҫ��ǰͳ����ÿ�����Զ��µı�������
 */
package org.dueam.hadoop.jobs.mysize;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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
 * @author buhan
 * 
 */
public class CountCatPro {

	
	

	
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, LongWritable> {

		private final static LongWritable one = new LongWritable(1);

		
		private static List<String> list1 = new ArrayList<String>();
		private static List<String> list2 = new ArrayList<String>();
		private static List<String> list3 = new ArrayList<String>();
		private static List<List<String>> all = new ArrayList<List<String>>();
		
		
		public  Map() {
			list1.add("10294727:148633469");
			list1.add("10294727:148633470");
			list1.add("10294727:148633472");

			list2.add("34301:29444");
			list2.add("34301:29445");
			list2.add("34301:3216779");
			list2.add("34301:130163");
			list2.add("34301:29964");

			list3.add("18551851:12652518");
			list3.add("18551851:130312");
			list3.add("18551851:6129887");
			list3.add("18551851:9834279");

			for (String str1 : list1) {
				for (String str2 : list2) {
					for (String str3 : list3) {
						List<String> list = new ArrayList<String>();
						list.add(str1);
						list.add(str2);
						list.add(str3);
						System.out.println(list.toString());
						all.add(list);
					}
				}
			}

			System.out.println("all.size():"+all.size());
		}
		
		public static String replacePro(String valueS) {

			if (StringUtils.isBlank(valueS)) {
				return valueS;
			}

			valueS.replaceAll("10294727:10294727", "10294727:148633469");
			valueS.replaceAll("10294727:148633473", "10294727:148633470");

			valueS.replaceAll("34301:11162412", "34301:29444");
			valueS.replaceAll("34301:3216780", "34301:3216779");

			valueS.replaceAll("18551851:14320260", "18551851:12652518");
			valueS.replaceAll("18551851:130318", "18551851:12652518");
			valueS.replaceAll("18551851:130313", "18551851:130312");
			valueS.replaceAll("18551851:5421384", "18551851:130312");
			valueS.replaceAll("18551851:3596436", "18551851:6129887");
			valueS.replaceAll("18551851:106225407", "18551851:6129887");
			valueS.replaceAll("18551851:148583812", "18551851:9834279");

			return valueS;

		}
		
		
		public void map(LongWritable key, Text value, OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException {

			String valueS = value.toString();
			
			
			// �滻���ȼ۵�����ֵID
			valueS = replacePro(valueS);

			if (StringUtils.isNotBlank(valueS)) {
				output.collect(new Text("aaa"), one);
				output.collect(new Text(all.size()+"all.size()"), one);
				for (List<String> list : all) {
					int n = 0;
					for (String str : list) {
						if (valueS.indexOf(str) == -1) {
							break;
						} else {
							n++;
						}
					}
					if (n == list.size()) {
						output.collect(new Text(list.toString()), one);
					}
				}

			} else {
				output.collect(new Text("ccc"), one);
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
		 * JobConf��map/reduce��job�����࣬��hadoop�������map-reduceִ�еĹ���
		 * ���췽����JobConf()��JobConf(Class exampleClass)��JobConf(Configuration
		 * conf)��
		 */
		JobConf conf = new JobConf(Mysize.class);
		conf.setJobName("Mysize_CountPro"); // ����һ���û������job����

		conf.setOutputKeyClass(Text.class); // Ϊjob�������������Key��
		conf.setOutputValueClass(LongWritable.class); // Ϊjob�������value��

		conf.setMapperClass(Map.class); // Ϊjob����Mapper��
		conf.setCombinerClass(Reduce.class); // Ϊjob����Combiner��
		conf.setReducerClass(Reduce.class); // Ϊjob����Reduce��

		conf.setInputFormat(TextInputFormat.class); // Ϊmap-reduce��������InputFormatʵ����
		conf.setOutputFormat(TextOutputFormat.class); // Ϊmap-reduce��������OutputFormatʵ����

		// InputFormat����map-reduce�ж�job�����붨�� setInputPaths():Ϊmap-reduce
		// job����·��������Ϊ�����б� setInputPath()��Ϊmap-reduce job����·��������Ϊ����б�

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf); // ����һ��job
	}

	

	

}
