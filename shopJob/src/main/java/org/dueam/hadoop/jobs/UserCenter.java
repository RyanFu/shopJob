package org.dueam.hadoop.jobs;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.util.ToolRunner;
import org.dueam.hadoop.common.HadoopTable;
import org.dueam.hadoop.common.SimpleMapReduce;
import org.dueam.hadoop.common.tables.AuctionAuctions;
import org.dueam.hadoop.common.tables.BmwUsers;
import org.dueam.hadoop.common.util.DateStringUtils;
import org.dueam.hadoop.common.util.Utils;

import java.io.IOException;

/**
 * 用户表相关信息监控
 * User: windonly
 * Date: 10-12-16 下午5:44
 */
public class UserCenter extends SimpleMapReduce {
    @Override
    protected String[] getInputPath(String[] args) {
        String[] input = HadoopTable.bmwUsersDelta(args[0]).getInputPath();
        System.out.println("input path => " + Utils.asString(input));
        return input;
    }

    @Override
    protected Class getInputFormat() {
        return HadoopTable.auction(null).getInputFormat();
    }

    @Override
    protected String getOutputPath(String[] args) {
        return "/group/tbdev/xiaodu/suoni/user_center/" + args[0];

    }


    @Override
    protected void doWork(String line, OutputCollector<Text, LongWritable> output) throws IOException {
        String[] _allCols = StringUtils.splitPreserveAllTokens(line, TAB);
        if(_allCols.length < 51){
            return;
        }

        String queryDate = DateStringUtils.format(inputArgs[0]);
        //昨天注册的用户
        if(Utils.isSameDay(queryDate,_allCols[BmwUsers.USER_REGDATE]))   {
            output.collect(Utils.mergeKey("total","reg"),new LongWritable(1));
            output.collect(Utils.mergeKey("suspended",_allCols[BmwUsers.SUSPENDED]),new LongWritable(1));

        }
        //昨天激活的用户
        if(Utils.isSameDay(queryDate,_allCols[BmwUsers.USER_SESSION_TIME]))   {
            output.collect(Utils.mergeKey("total","active"),new LongWritable(1));
            output.collect(Utils.mergeKey("user_active",_allCols[BmwUsers.USER_ACTIVE]),new LongWritable(1));
        }

    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),
                new UserCenter(), args);
        System.exit(res);
    }


}
