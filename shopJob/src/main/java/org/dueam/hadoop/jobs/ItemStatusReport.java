package org.dueam.hadoop.jobs;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.dueam.hadoop.common.tables.AuctionAuctions;
import org.dueam.hadoop.utils.TaobaoPath;

/**
 * 追踪多天新发布商品的增长情况
 *
 */
public class ItemStatusReport extends Configured implements Tool {
	public int run(String[] args) throws Exception {
		if (args.length < 1) {
			args = new String[] { TaobaoPath.now() };
			System.out.println("ERROR: Please Enter Date , eg. 20100507  now use default!"
							+ TaobaoPath.now());
		}

		JobConf conf = new JobConf(getConf(), ItemStatusReport.class);
		conf.setJobName("ItemStatusReport-" + System.currentTimeMillis());

		String date = args[0];
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(TaobaoPath.getOutput("item_status_report", date))) {
			System.out.println("ERROR: You has finish this job at this day :  "
					+ date + " [ "
					+ TaobaoPath.getOutput("item_status_report", date) + " ] ");
			return -1;
		}
		String queryDate = TaobaoPath.newDates(date, -30);

		if (args.length > 1) {
			queryDate = args[1];
		}

		conf.set("user.date", queryDate);
		conf.setNumReduceTasks(100);
		conf.setMapperClass(MapClass.class);
		conf.setReducerClass(LongSumReducer.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(LongWritable.class);
		TextInputFormat.addInputPath(conf, TaobaoPath.hiveAuctionAuctions(date));
        conf.setCombinerClass(LongSumReducer.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(LongWritable.class);

		FileOutputFormat.setOutputPath(conf, TaobaoPath.getOutput(
				"item_status_report", date));

		JobClient.runJob(conf);

		return JobClient.SUCCESS;
	}

	public static class MapClass extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, LongWritable> {
		private LongWritable one = new LongWritable(1);
		final char TAB = (char) 0x01;

		// 使用者 时间 preurl cururl
		public void map(LongWritable key, Text value,
				OutputCollector<Text, LongWritable> out, Reporter reporter)
				throws IOException {
			String line = value.toString();
			String[] _allCols = StringUtils.splitPreserveAllTokens(line, TAB);
			if (_allCols.length < 41) {
				return;
			}
			String gmtCreated = _allCols[AuctionAuctions.OLD_STARTS];
			String status = _allCols[AuctionAuctions.AUCTION_STATUS];

			out.collect(new Text("ALL" + "\t"
					+ status), one);

			if (gmtCreated != null && gmtCreated.compareTo(date) >= 0) {
				out.collect(new Text(gmtCreated.substring(0, 10) + "\t"
						+ status), one);
			}

		}

		private String date;

		// 设置用户输入参数
		public void configure(JobConf job) {
			String tmp = job.get("user.date");
			date = tmp.substring(0, 4) + "-" + tmp.substring(4, 6) + "-"
					+ tmp.substring(6, 8)+" 00:00:00";
			System.out.println("configured : date = " + date);
		}
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new ItemStatusReport(),
				args);
		System.exit(res);
	}
}
