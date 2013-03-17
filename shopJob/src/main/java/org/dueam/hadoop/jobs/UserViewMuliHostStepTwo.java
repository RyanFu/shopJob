package org.dueam.hadoop.jobs;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.dueam.hadoop.common.HadoopTable;
import org.dueam.hadoop.common.tables.Atpanel;
import org.dueam.hadoop.common.util.DateStringUtils;
import org.dueam.hadoop.common.util.Utils;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * User: windonly
 * Date: 11-1-4 下午1:14
 */
public class UserViewMuliHostStepTwo extends Configured implements Tool, Reducer<Text, Text, Text, Text>, Mapper<Object, Text, Text, Text> {

    public int run(String[] args) throws Exception {
        if (args.length < 1) {
            args = new String[]{DateStringUtils.now()};
            System.out.println("ERROR: Please Enter Date , eg. 20101010 ! now use default => " + DateStringUtils.now());
        }

        JobConf config = new JobConf(getConf(), getClass());
        config.set("user.args", Utils.asString(args));

        config.setJobName(getClass() + "-" + System.currentTimeMillis());
        config.setNumReduceTasks(100);
        config.setMapperClass(getClass());
        config.setReducerClass(getClass());
        config.setInputFormat(getInputFormat());
        config.setMapOutputKeyClass(Text.class);
        config.setMapOutputValueClass(Text.class);

        //add input paths
        for (String path : getInputPath(args)) {
            if (TextInputFormat.class.equals(getInputFormat())) {
                TextInputFormat.addInputPath(config, new Path(path));
            } else if (SequenceFileInputFormat.class.equals(getInputFormat())) {
                SequenceFileInputFormat.addInputPath(config, new Path(path));
            }
        }

        config.setOutputKeyClass(Text.class);
        config.setOutputValueClass(Text.class);

        //if output path exists then return
        FileSystem fs = FileSystem.get(config);
        Path outputPath = new Path(getOutputPath(args));
        FileOutputFormat.setOutputPath(config, outputPath);

        if (!fs.exists(outputPath)) {
            JobClient.runJob(config);
        } else {
            System.out.println("You has finished this job today ! " + outputPath);
        }

        return JobClient.SUCCESS;
    }

    protected String[] getInputPath(String[] args) {
        //String[] input = new String[]{HadoopTable.atpanel(args[0])};

        String[] input = new String[]{"/group/tbdev/xiaodu/suoni/user_view_muli_host_setp_one_filter/" + args[0]};
        if (args.length > 1 && "false".equals(args[1])) {
            input = new String[]{"/group/tbdev/xiaodu/suoni/user_view_muli_host_setp_one_filter_all/" + args[0]};
        }
        System.out.println("input path => " + Utils.asString(input));
        return input;
    }

    protected Class getInputFormat() {
        return TextInputFormat.class;
    }

    protected String getOutputPath(String[] args) {
        if (args.length > 1 && "false".equals(args[1])) {
            return "/group/tbdev/xiaodu/suoni/user_view_muli_host_setp_two_all/" + args[0];
        }
        return "/group/tbdev/xiaodu/suoni/user_view_muli_host_setp_two/" + args[0];
    }

    public void map(Object key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        String[] _allCols = StringUtils.splitPreserveAllTokens(value.toString(), TAB);
        output.collect(new Text(_allCols[0]), value);
    }

    protected String[] inputArgs;

    public void configure(JobConf job) {
        this.inputArgs = Utils.asArray(job.get("user.args"));
    }

    final static String DATETIME_STYLE = "yyyyMMddhhmmss";

    public static boolean isSameTime(String begin, String end) {
        long s = Math.abs(DateStringUtils.second(begin, end, DATETIME_STYLE));
        return s <= 1800;
    }

    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        List<String[]> _temp = new ArrayList<String[]>();
        int count = 0;
        while (values.hasNext()) {
            Text _out = values.next();
            String[] tokens = StringUtils.splitPreserveAllTokens(_out.toString(), TAB);
            _temp.add(tokens);
            if(count ++ > 100000)break;
        }

        /**
         * 分组算法
         * FOREACH ALL_DATA
         *  IF IN INDEX THEN
         *    UPDATE INDEX AND INSERT DATA
         *  ELSE
         *    FOREACH SUB_DATA
         *      MAKE INDEX AND SET FIND'S DATA AS NULL
         */
        List<List<String[]>> dataList = new ArrayList<List<String[]>>();
        List<StringBuffer> indexList = new ArrayList<StringBuffer>();
        boolean muliHost = false;
        for (int posI = 0; posI < _temp.size(); posI++) {
            String[] array = _temp.get(posI);
            if (array == null) continue;

            String mid = array[2];
            String ip = array[3];
            boolean hasIndex = false;
            for (int i = 0; i < indexList.size(); i++) {
                StringBuffer index = indexList.get(i);
                if (index.indexOf("|" + mid + "|") >= 0 || index.indexOf("|" + ip + "|") >= 0) {
                    if (index.indexOf("|" + mid + "|") < 0) {
                        index.append('|').append(mid).append('|');
                    }

                    if (index.indexOf("|" + ip + "|") < 0) {
                        index.append('|').append(ip).append('|');
                    }
                    dataList.get(i).add(array);
                    hasIndex = true;
                    break;
                }

            }
            if (!hasIndex) {
                StringBuffer index = new StringBuffer("|" + mid + "|" + ip + "|");
                List<String[]> _tmp = new ArrayList<String[]>();
                _tmp.add(array);
                for (int k = posI + 1; k < _temp.size(); k++) {
                    String[] _newArray = _temp.get(k);
                    if (_newArray == null) {
                        continue;
                    }
                    String _mid = _newArray[2];
                    String _ip = _newArray[3];
                    if (index.indexOf("|" + _mid + "|") >= 0 || index.indexOf("|" + _ip + "|") >= 0) {
                        if (index.indexOf("|" + _mid + "|") < 0) {
                            index.append('|').append(_mid).append('|');
                        }

                        if (index.indexOf("|" + _ip + "|") < 0) {
                            index.append('|').append(_ip).append('|');
                        }
                        //_tmp.add(_newArray);
                        //_temp.set(k,null);
                    }
                }
                indexList.add(index);
                dataList.add(_tmp);
            }
            if(dataList.size() > 1){
                muliHost = true;
                break;
            }
        }
        for(String[] _array : _temp){
            output.collect(key,Utils.mergeKey(_array[1],_array[2],_array[3],_array[4]));
        }

    }

    final static char TAB = (char) 0x09;


    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),
                new UserViewMuliHostStepTwo(), args);
        System.exit(res);
    }


    public void close() throws IOException {

    }


}
