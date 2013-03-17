package org.dueam.hadoop.bp.jobs.life;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.dueam.hadoop.common.MapReduce;

/**
 * 卡宝access日志统计
 * 
 *  int total_pv //总共的pv
 *	int alipay_pv //支付宝pv
 *	int market_pv //市场pv
 *	
 *	int return_200 //返回200的请求
 *	int return_302 //返回300的请求
 * @author longlong
 *
 */
public class KabaoAccessJobs extends MapReduce<Text, Text> {

	//private final char SPACE = 0x20;//空格

	@Override
	protected void setup(String[] args) {}

	@Override
	public void map(Object key, Text value, OutputCollector<Text, Text> output,
			Reporter reporter) throws IOException {

		//处理输入的日志记录
		ArrayList<String> line_arr = new ArrayList<String>();
		String[] input_strs = value.toString().split(" ");
		boolean istoken = false;
		String tokenstr = "";
		for(String c : input_strs){
			

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

		//String[] _allCols = StringUtils.splitPreserveAllTokens(value.toString(), SPACE);

	}

	@Override
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
					throws IOException {
		long count = 0;//计算总数
		while(values.hasNext()){
			String valuetemp = values.next().toString();
			long temp = Long.parseLong(valuetemp);
			count += temp;
		}

		output.collect(key, new Text(count+""));

	}

	public static  Date getNextDay(Date date) {
		long now = date.getTime();
		long yestoday = now - 24*60*60*1000;

		return new Date(yestoday);
	}

	public static void main(String args[]) throws Exception{

//		Date date = new Date();
//		date = getNextDay(date);
//		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
//		String datestr = sdf.format(date);
//
//		String input = "/group/tbptd-dev/longlong/kabao_access/" + datestr + "/00";
//		String output = "/group/tbptd-dev/longlong/kabao_access_temp/" + datestr ;
//		int taskNum = 100;
//
//		JobConf conf = new JobConf(KabaoAccessJobs.class);
//		conf.setJobName("kabao_access");
//
//
//		FileSystem.get(conf).delete(new Path(output), true);
//
//		conf.setOutputKeyClass(Text.class);
//		conf.setOutputValueClass(Text.class);
//
//		conf.setMapperClass(KabaoAccessJobs.class);
//		conf.setReducerClass(KabaoAccessJobs.class);
//
//		conf.setInputFormat(TextInputFormat.class);
//		conf.setOutputFormat(TextOutputFormat.class);
//
//		FileInputFormat.setInputPaths(conf, new Path(input));
//		FileOutputFormat.setOutputPath(conf, new Path(output));
//
//		conf.setNumReduceTasks(taskNum);
//		JobClient.runJob(conf);

		// int res = ToolRunner.run(new Configuration(),
		//          new KabaoAccessJobs(), args);
		//System.exit(res);

		String value = "0       110.75.145.2 5543 - [29/Jan/2013:16:56:41 +0800] \"POST http://quan.taobao.com/api/kaBao.do\" 200 114 \"-\" \"Mozilla/4.0\" 0.006";
		
		ArrayList<String> line_arr = new ArrayList<String>();
		String[] input_strs = value.split(" ");
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
				//System.out.println("String : " + tokenstr);
				line_arr.add(tokenstr);
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
		
		for(int i = 0; i < line_arr.size(); i ++){
			if(i == 5){//请求url
				String url = line_arr.get(i);
				if("\"POST http://quan.taobao.com/api/kaBao.do\"".equals(url)){//支付宝请求
					System.out.println("alipay_pv");
				}else{//market请求
					System.out.println("market_pv");
				}
			}
			if(i == 6){//http返回状态
				String status = line_arr.get(i);
				if("200".equals(status)){
					System.out.println("200");
				}
				if("302".equals(status)){
					System.out.println("302");
				}
			}
		}

	}


}
