package org.dueam.hadoop.jobs;

import java.io.IOException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.lib.LongSumReducer;
import org.apache.hadoop.util.ToolRunner;
import org.dueam.hadoop.common.HadoopTable;
import org.dueam.hadoop.common.MyFeedMapReduce;
import org.dueam.hadoop.common.util.DateStringUtils;
import org.dueam.hadoop.common.util.Utils;

public class BackyardFeedCenter extends MyFeedMapReduce{

	@Override
	protected String[] getInputPath(String[] args) {
		String[] input = HadoopTable.auction(args[0]).getInputPath();
		System.out.println("input path => " + Utils.asString(input));
		return input;
	}

	@Override
	protected Class getInputFormat() {
		return HadoopTable.auction(null).getInputFormat();
	}

	@Override
	protected String getOutputPath(String[] args) {
		return "/group/tbptd-dev/mengka.hyy/feed/" + args[0];
	}

	@Override
	protected void doWork(String line,
			OutputCollector<Text, LongWritable> output) throws IOException {
		String[] aaStrings = StringUtils.splitPreserveAllTokens(line, "\t");
		
		String idString = aaStrings[0];
		
		String typeString = aaStrings[2];
		String feedIdString = aaStrings[3];
		
		String sellerIdString = aaStrings[4];
		String userIdString = aaStrings[5];
		String userNick = aaStrings[6];
		
		String favorString = aaStrings[7];
		String shareString = aaStrings[8];
		String commentString = aaStrings[9];
		
		String content = aaStrings[11];
		
		
		long favor = 0;
		long share = 0;
		long comment = 0;
		if(StringUtils.isNumeric(favorString)){
			favor = Long.parseLong(favorString);
		}
		if(StringUtils.isNumeric(shareString)){
			share = Long.parseLong(shareString);
		}
		if(StringUtils.isNumeric(commentString)){
			comment = Long.parseLong(commentString);
		}
		//活动的feed表
		if("2".equals(typeString)){
			output.collect(Utils.mergeKey("favor", userNick),
					new LongWritable(favor));
			output.collect(Utils.mergeKey("share", userNick),
					new LongWritable(share));
			output.collect(Utils.mergeKey("comment", userNick),
					new LongWritable(comment));
		}
	}
	
	@Override
	protected void beforeRun(String[] args, JobConf config) {
		config.setCombinerClass(LongSumReducer.class);
	}
	protected void setup(String[] args) {
		this.taskNum = 10;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new BackyardFeedCenter(), args);
		System.exit(res);
	}

}
