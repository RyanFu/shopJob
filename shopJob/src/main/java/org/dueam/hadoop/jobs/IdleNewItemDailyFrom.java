package org.dueam.hadoop.jobs;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.dueam.hadoop.common.tables.AuctionAuctions;
import org.dueam.hadoop.utils.TaobaoPath;

import java.io.IOException;

/**
 * 统计商品来源
 *
 * @author suoni
 */
public class IdleNewItemDailyFrom extends Configured implements Tool {
    public int run(String[] args) throws Exception {

        if (args.length < 1) {
            args = new String[]{TaobaoPath.now()};
            System.out.println("ERROR: Please Enter Date , eg. 20100507  now use default!"
                    + TaobaoPath.now());
        }

        JobConf conf = new JobConf(getConf(), IdleNewItemDailyFrom.class);
        conf.setJobName("IdleNewItemDailyFrom-" + System.currentTimeMillis());

        String date = args[0];
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(TaobaoPath.getOutput("idle_new_item_daily_from", date))) {
            System.out.println("ERROR: You has finish this job at this day :  "
                    + date + " [ "
                    + TaobaoPath.getOutput("idle_new_item_daily_from", date) + " ] ");
            return -1;
        }

        conf.set("user.date", date);
        conf.setNumReduceTasks(1);
        conf.setMapperClass(MapClass.class);
        conf.setReducerClass(LongSumReducer.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(LongWritable.class);
        TextInputFormat.addInputPath(conf, TaobaoPath.hiveAuctionAuctions(date));

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(LongWritable.class);

        FileOutputFormat.setOutputPath(conf, TaobaoPath.getOutput(
                "idle_new_item_daily_from", date));

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
            if (!"8".equals(_allCols[AuctionAuctions.STUFF_STATUS])) {
                return;
            }

            String gmtCreated = _allCols[AuctionAuctions.OLD_STARTS];
            if (gmtCreated != null && gmtCreated.indexOf(date) >= 0) {
                String status = _allCols[AuctionAuctions.AUCTION_STATUS];
                String attr = _allCols[AuctionAuctions.FEATURES];
                String from = TaobaoPath.getValue(attr, "source", null);
                out.collect(new Text(from + "\t" + status), one);
                out.collect(new Text("cat\t" + _allCols[AuctionAuctions.CATEGORY]), one);
            }


        }

        private String date;

        public void configure(JobConf job) {
            String tmp = job.get("user.date");
            date = tmp.substring(0, 4) + "-" + tmp.substring(4, 6) + "-"
                    + tmp.substring(6, 8);
            System.out.println("configured : date = " + date);
        }
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new IdleNewItemDailyFrom(),
                args);
        System.exit(res);
    }
}
