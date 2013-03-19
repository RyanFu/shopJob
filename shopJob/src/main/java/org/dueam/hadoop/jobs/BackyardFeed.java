package org.dueam.hadoop.jobs;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.dueam.hadoop.utils.JobConstants;
import org.dueam.hadoop.utils.JobStringUtil;
import org.dueam.hadoop.utils.JobUtil;

public class BackyardFeed extends Configured implements Tool {

	StringBuffer sb1 = new StringBuffer();

	String FEED_INPUT_PATH_1 = sb1.append("/group/taobao/taobao/dw/stb/")
			.append(getCurrentDate()).append("/hy_feeds_mysql_virtual_db/")
			.toString();

	private static final String OUTPUT_PATH = "/group/tbptd-dev/mengka.hyy/feed";

	public static String getCurrentDate() {
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.DAY_OF_MONTH, -1);
		DateFormat SIMPLE_DATA_FORMAT = new SimpleDateFormat("yyyyMMdd");
		return SIMPLE_DATA_FORMAT.format(cal.getTime());
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int exitCode = ToolRunner.run(new BackyardFeed(), args);
		System.exit(exitCode);
	}

	public int run(String[] arg0) throws Exception {
		JobConf jobConf = buildConf(new Configuration());
		// 执行
		JobClient.runJob(jobConf);
		return 0;
	}

	public JobConf buildConf(Configuration configuration) throws IOException {
		JobConf jobConf = new JobConf(configuration);
		jobConf.setJobName("BackyardFeed");
		// 判断输出目录是否存在，存在则DEL
		try {
			JobUtil.delDir(OUTPUT_PATH, jobConf);
		} catch (Exception e) {
			// TODO: handle exception
			System.out
					.println("-----> JobUtil.isExist Error:" + e.getMessage());
		}

		// feed数据输入
		MultipleInputs.addInputPath(jobConf, new Path(FEED_INPUT_PATH_1),
				SequenceFileInputFormat.class, FeedAuction.class);
		FileOutputFormat.setOutputPath(jobConf, new Path(OUTPUT_PATH));

		// reducer的设置
		jobConf.setNumReduceTasks(20);
		jobConf.setReducerClass(FeedAuctionReducer.class);

		// 输出的配置: map的输出类型不是BDB, 需要重新设置下,其他的job也是一样的
		jobConf.setOutputFormat(TextOutputFormat.class);

		jobConf.setMapOutputKeyClass(Text.class);
		jobConf.setMapOutputValueClass(Text.class);
		jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(Text.class);
		return jobConf;
	}
}

/**
 *  读取feed的数据
 * 
 * @author Administrator
 *
 */
class FeedAuction extends MapReduceBase implements
Mapper<Writable, Text, Text, Text> {
	public void map(Writable key, Text value,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		
		String feedString = value.toString();
		if (feedString != null) {
			String[] aaStrings = JobStringUtil.splitPreserveAllTokens(
					feedString, JobConstants.AUCTION_SP);
			try {
				
				String idString = aaStrings[0];
				
				String typeString = aaStrings[2];
				String feedIdString = aaStrings[3];
				
				String favorString = aaStrings[7];
				String shareString = aaStrings[8];
				String commentString = aaStrings[9];
				
				String tlag=typeString+"\t"+feedIdString+"\t"+favorString+"\t"+shareString+"\t"+commentString;
				
				output.collect(new Text(idString), new Text(tlag));
				
			}catch(Exception e){
				//
			}
		}
		
	}
}

/**
 *  reduce操作
 * 
 * @author Administrator
 *
 */
class FeedAuctionReducer extends MapReduceBase implements
Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		
		String feedString = null;
		
		int count = 0;
		while (values.hasNext()) {
			count++;
			feedString = values.next().toString();
	
		}
		
		if(count>=2){
			//对超过2个的map的处理
		}
		
		if(feedString!=null){
			Text fullaaText = null;
			fullaaText = new Text(feedString);
			output.collect(new Text(key), fullaaText);
		}
	}
}