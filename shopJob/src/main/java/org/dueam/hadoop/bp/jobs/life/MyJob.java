package org.dueam.hadoop.bp.jobs.life;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;

import org.apache.hadoop.fs.FileSystem;
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
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class MyJob {

	public static class KaobaoMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

		//		//1089545142 86a6d7dab30c476487e53605438e0ee5 460200 d:5,b:5,c:5,a:5
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			//			String simpleStr = value.toString();
			//			simpleStr = simpleStr.replaceAll("\"", "");
			//			String[] valueStr = simpleStr.split(",");
			//			if (valueStr.length >= 3) {
			//				String userId = valueStr[0];
			//				String storeId = valueStr[1];
			//				String city = valueStr[2];
			//				output.collect(new Text(city+"_"+userId), new Text(storeId));
			//			}
			//		}

			//System.out.println( value.toString());
			//处理输入的日志记录
			ArrayList<String> line_arr = new ArrayList<String>();
			String[] input_strs = value.toString().split(" ");
			boolean istoken = false;
			String tokenstr = "";
			for(String c : input_strs){
				if(c == null || "".equals(c.trim()))continue;
				if(c.startsWith("\"")&&!c.endsWith("\"") || c.startsWith("[")&&!c.endsWith("]")){
					tokenstr = tokenstr + c;
					istoken = true; 
					continue;
				}
				if(c.endsWith("\"")&&!c.startsWith("\"") || c.endsWith("]")&&!c.startsWith("[") ){
					tokenstr = tokenstr + " " +  c;
					istoken = false; 
					//获得tokenstr
					line_arr.add(tokenstr);
					//System.out.println("String : " + tokenstr);
					tokenstr = "";
					continue;
				}
				if(istoken){
					tokenstr = tokenstr + " " +  c;
					continue;
				}
				//获得tokenstr
				//System.out.println("String : " + c);
				line_arr.add(c);

			}

			output.collect(new Text("total_pv"), new Text("1"));
			for(int i = 0; i < line_arr.size(); i ++){
				if(i == 4){//请求url
					String url = line_arr.get(i);
					if("\"POST http://quan.taobao.com/api/kaBao.do\"".equals(url)){//支付宝请求
						output.collect(new Text("alipay_pv"), new Text("1"));
					}else{//market请求
						output.collect(new Text("market_pv"), new Text("1"));
					}
				}
				if(i == 5){//http返回状态
					String status = line_arr.get(i);
					if("200".equals(status)){
						output.collect(new Text("return_200"), new Text("1"));
					}
					if("302".equals(status)){
						output.collect(new Text("return_302"), new Text("1"));
					}
				}
			}

		}
		
	}
	
	public static class KaobaoReduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			long count = 0;//计算总数
			while(values.hasNext()){
				String valuetemp = values.next().toString();
				long temp = Long.parseLong(valuetemp);
				count += temp;
			}

			output.collect(key, new Text(count+""));
		}
	}
	
	public static  Date getNextDay(Date date) {
		long now = date.getTime();
		long yestoday = now - 24*60*60*1000;

		return new Date(yestoday);
	}
	
	public static void main(String[] args) {
		Date date = new Date();
		date = getNextDay(date);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		String datestr = sdf.format(date);

		String input = "/group/tbptd-dev/longlong/kabao_access/" + datestr + "/00/";
		String output = "/group/tbptd-dev/longlong/kabao_access_temp/" + datestr ;

		JobConf conf = new JobConf(MyJob.class);
		conf.setJobName("KaobaoAnalysis");

		try {
			FileSystem.get(conf).delete(new Path(output), true);
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(Text.class);

			conf.setMapperClass(KaobaoMap.class);
			conf.setReducerClass(KaobaoReduce.class);

			conf.setInputFormat(SequenceFileInputFormat.class);
			conf.setOutputFormat(TextOutputFormat.class);

			FileInputFormat.setInputPaths(conf, new Path(input));
			FileOutputFormat.setOutputPath(conf, new Path(output));

			conf.setNumReduceTasks(50);
			JobClient.runJob(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
