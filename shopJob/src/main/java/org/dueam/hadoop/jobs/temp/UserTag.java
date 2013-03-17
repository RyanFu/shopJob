package org.dueam.hadoop.jobs.temp;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.LongSumReducer;
import org.apache.hadoop.util.ToolRunner;
import org.dueam.hadoop.common.HadoopTable;
import org.dueam.hadoop.common.SimpleMapReduce;
import org.dueam.hadoop.common.tables.BmwUsers;
import org.dueam.hadoop.common.tables.BmwUsersExtra;
import org.dueam.hadoop.common.util.DateStringUtils;
import org.dueam.hadoop.common.util.Utils;

import java.io.IOException;

import static org.dueam.hadoop.common.tables.Cart.*;
import static org.dueam.hadoop.conf.Cart.getArea;

/**
 * User: windonly
 * Date: 11-1-6 ÏÂÎç4:45
 */
public class UserTag extends SimpleMapReduce {
    @Override
    protected String[] getInputPath(String[] args) {
        return new String[]{args[1]};
    }

    @Override
    protected Class getInputFormat() {

        return TextInputFormat.class;
    }

    @Override
    protected String getOutputPath(String[] args) {
        return "/group/tbdev/xiaodu/suoni/temp/" + args[0];

    }

    @Override
    protected void doWork(String line, OutputCollector<Text, LongWritable> output) throws IOException {
        if (StringUtils.equals("bmw_users", this.inputArgs[0])) {
            String[] _allCols = StringUtils.splitPreserveAllTokens(line, TAB);
            if (_allCols.length < BmwUsers.VERSION) {
                return;
            }
            tags("promoted_type", NumberUtils.toLong(_allCols[BmwUsers.PROMOTED_TYPE]), output);
        } else if (StringUtils.equals("bmw_users_extra", this.inputArgs[0])) {
            String[] _allCols = StringUtils.splitPreserveAllTokens(line, CTRL_A);
            if (_allCols.length < BmwUsersExtra.version) {
                return;
            }
            tags("user_tag", NumberUtils.toLong(_allCols[BmwUsersExtra.user_tag]), output);
            tags("user_tag2", NumberUtils.toLong(_allCols[BmwUsersExtra.user_tag2]), output);
            tags("user_tag3", NumberUtils.toLong(_allCols[BmwUsersExtra.user_tag3]), output);
            tags("user_tag4", NumberUtils.toLong(_allCols[BmwUsersExtra.user_tag4]), output);
        }

    }

    protected static void tags(String key, long value, OutputCollector<Text, LongWritable> output) throws IOException {
        int pos = 1;
        for (long pow : twoPow) {
            if (value < pow) return;
            if ((value & pow) > 0) {
                Text outKey = Utils.mergeKey(key, String.valueOf(pow), String.valueOf(pos));
                output.collect(outKey, ONE);
            }
            pos++;
        }
    }
    @Override
    protected void beforeRun(String[] args, JobConf config) {
        if (StringUtils.equals("bmw_users_extra", args[0])) {
            config.setInputFormat(SequenceFileInputFormat.class);
        }
        config.setCombinerClass(LongSumReducer.class);
    }

    static long[] twoPow = new long[]{1L, 2L, 4L, 8L, 16L, 32L, 64L, 128L, 256L, 512L, 1024L, 2048L, 4096L, 8192L, 16384L, 32768L, 65536L, 131072L, 262144L, 524288L, 1048576L, 2097152L, 4194304L, 8388608L, 16777216L, 33554432L, 67108864L, 134217728L, 268435456L, 536870912L, 1073741824L, 2147483648L, 4294967296L, 8589934592L, 17179869184L, 34359738368L, 68719476736L, 137438953472L, 274877906944L, 549755813888L, 1099511627776L, 2199023255552L, 4398046511104L, 8796093022208L, 17592186044416L, 35184372088832L, 70368744177664L, 140737488355328L, 281474976710656L, 562949953421312L, 1125899906842624L, 2251799813685248L, 4503599627370496L, 9007199254740992L, 18014398509481984L, 36028797018963968L, 72057594037927936L, 144115188075855872L, 288230376151711744L, 576460752303423488L, 1152921504606846976L, 2305843009213693952L, 4611686018427387904L};

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),
                new UserTag(), args);
        System.exit(res);
    }
}
