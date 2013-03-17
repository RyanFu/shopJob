package org.dueam.hadoop.jobs;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.util.ToolRunner;
import org.dueam.hadoop.common.HadoopTable;
import org.dueam.hadoop.common.SimpleMapReduce;
import org.dueam.hadoop.common.tables.BmwUsers;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.hadoop.utils.GBKMap;

import java.io.IOException;

/**
 * User: windonly
 * Date: 11-3-30 ÏÂÎç3:24
 */
public class UserTemp extends SimpleMapReduce {
    @Override
    protected String[] getInputPath(String[] args) {
        String[] input = HadoopTable.bmwUsers(args[0]).getInputPath();
        System.out.println("input path => " + Utils.asString(input));
        return input;
    }

    protected void setup(String[] args) {
        this.taskNum = 100;
        System.out.println("config task num : " + taskNum);
    }


    @Override
    protected Class getInputFormat() {
        return HadoopTable.bmwUsers(null).getInputFormat();
    }

    @Override
    protected String getOutputPath(String[] args) {
        return "/group/tbdev/xiaodu/suoni/user_nick/" + args[0];

    }


    @Override
    protected void doWork(String line, OutputCollector<Text, LongWritable> output) throws IOException {
        String[] _allCols = StringUtils.splitPreserveAllTokens(line, TAB);
        if (_allCols.length < 51) {
            return;
        }

        String nick = _allCols[BmwUsers.NICK];
        if (isHasUpperChar(nick)) {
            output.collect(Utils.mergeKey(_allCols[BmwUsers.USER_ID], nick, _allCols[BmwUsers.REG_DATE]), ONE);
        }


    }

    static boolean isHasUpperChar(String nick) {
        String gbk = GBKMap.covGBKT2S(nick);
        return !StringUtils.equals(nick,gbk);
//        if (nick == null) return false;
//        for (char c : nick.toCharArray()) {
//            if (c >= 'A' && c <= 'Z') {
//                return true;
//            }
//        }
//        return false;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),
                new UserTemp(), args);
        System.exit(res);
    }
}
