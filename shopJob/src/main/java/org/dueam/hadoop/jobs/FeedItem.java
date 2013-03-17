package org.dueam.hadoop.jobs;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.util.ToolRunner;
import org.dueam.hadoop.common.HadoopTable;
import org.dueam.hadoop.common.MapReduce;
import org.dueam.hadoop.common.Table;
import org.dueam.hadoop.common.tables.AuctionAuctions;
import org.dueam.hadoop.common.tables.FeedReceive;
import org.dueam.hadoop.common.util.DateStringUtils;
import org.dueam.hadoop.common.util.Utils;

import java.io.IOException;
import java.util.*;

/**
 * 通过评价挖掘好商品
 */
public class FeedItem extends MapReduce<Text, Text> {

    @Override
    protected void setup(String[] args) {
        this.taskNum = 100;
        Table table = HadoopTable.feedReceive(args[0], "50");
        this.input = new String[0];
        this.inputFormat = table.getInputFormat();
        this.output = "/group/tbbp/suoni/feed_receive/" + args[0];

    }

    @Override
    protected void beforeRun(String[] args, JobConf config) {
        Table table = HadoopTable.feedReceive(args[0], "50");
        for (String path : table.getInputPath()) {
            MultipleInputs.addInputPath(config, new Path(path), table.getInputFormat(), getClass());
        }
        Table item = HadoopTable.auctionOnline(args[0]);
        for (String path : item.getInputPath()) {
            MultipleInputs.addInputPath(config, new Path(path), item.getInputFormat(), AuctionMapper.class);
        }
        config.setMapOutputValueClass(Text.class);
    }

    public static class AuctionMapper extends MapReduceBase implements Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            String[] _allCols = StringUtils.splitPreserveAllTokens(value.toString(), CTRL_A);
            if ("1512".equals(_allCols[AuctionAuctions.CATEGORY]) && true) {
                output.collect(new Text(_allCols[AuctionAuctions.AUCTION_ID]), new Text("ONLINE"));
            } else {
                output.collect(new Text(_allCols[AuctionAuctions.AUCTION_ID]), new Text("BREAK"));
            }
        }
    }

    @Override
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        String itemId = key.toString();
        String title = null;
        List<String> rateList = new ArrayList<String>();
        int max = 200;
        boolean isOnline = true;
        while (values.hasNext()) {
            String line = values.next().toString();
            if (line.startsWith("ONLINE")) {
                isOnline = true;
                continue;
            }

            if (line.startsWith("BREAK")) {
                return;
            }
            if (rateList.size() > max) {
                String[] _cols = StringUtils.splitPreserveAllTokens(line, TAB);
                String rater = _cols[0];
                String rate = _cols[1];
                String auctionTitle = _cols[2];
                String feedback = _cols[3];
                String feedbackIp = _cols[4];

                if (title == null) {
                    title = auctionTitle;
                }

                int feedRate = rate(rater, rate, feedback, feedbackIp);
                if (feedRate > 1) {
                    rateList.add(feedRate + "^" + rater + "^" + rate + "^" + feedback);
                }

            }

        }
        Collections.sort(rateList, new Comparator<String>() {
            public int compare(String o1, String o2) {
                String[] col1 = StringUtils.splitPreserveAllTokens(o1, '^');
                String[] col2 = StringUtils.splitPreserveAllTokens(o2, '^');
                return NumberUtils.toInt(col1[0]) - NumberUtils.toInt(col2[0]);
            }
        });
        if (rateList.size() > 30) {
            rateList = rateList.subList(0, 30);
        }
        if (!rateList.isEmpty() && isOnline)
            output.collect(key, Utils.mergeKey(title, StringUtils.join(rateList, ',')));

    }

    /**
     * 评测评价内容
     *
     * @param rater
     * @param rate
     * @param feedback
     * @param feedbackIp
     * @return
     */
    public int rate(String rater, String rate, String feedback, String feedbackIp) {
        int feedRate = 0;
        //根据字数判断评价质量 ： 0-5，6-10,11-20,21 - => 1,2,4,8
        if (true) {
            int len = StringUtils.length(feedback);
            if (len > 20) {
                feedRate += 8;
            } else if (len > 10) {
                feedRate += 4;
            } else if (len > 5) {
                feedRate += 2;
            } else {
                feedRate += 1;
            }
        }

        if (true) {
            int gbkCount = 0;
            int englishCount = 0;
            int spanCount = 0;
            for (byte b : feedback.getBytes()) {
                if (b < 0) {
                    gbkCount++;
                } else if ((b >= '0' && b <= '9') || (b >= 'A' && b <= 'Z') || (b >= 'a' && b <= 'z')) {
                    englishCount++;
                } else {
                    spanCount++;
                }
            }

            int span = spanCount * 100 / feedback.length();
            if (span < 20) {
                feedRate -= 1;
            } else if (span < 40) {
                feedRate -= 2;
            } else if (span < 80) {
                feedRate -= 4;
            } else {
                feedRate -= 8;
            }
        }

        return feedRate;
    }

    @Override
    public void map(Object key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        String[] _allCols = StringUtils.splitPreserveAllTokens(value.toString(), TAB);
        if (_allCols.length < 30) {
            return;
        }

        String queryDate = DateStringUtils.format(inputArgs[0]);
        if ("0".equals(_allCols[FeedReceive.rater_type]) && "1".equals(_allCols[FeedReceive.validfeedback])) {
            String itemId = _allCols[FeedReceive.auc_num_id];
            String rater = _allCols[FeedReceive.rater_user_nick];
            String rate = _allCols[FeedReceive.rate];
            String auctionTitle = _allCols[FeedReceive.auction_title];
            String feedback = _allCols[FeedReceive.feedback];
            String feedbackIp = _allCols[FeedReceive.feedback_ip];
            output.collect(new Text(itemId), Utils.mergeKey(rater, rate, auctionTitle, feedback, feedbackIp));
        }
    }


    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),
                new FeedItem(), args);
        System.exit(res);
    }


}
