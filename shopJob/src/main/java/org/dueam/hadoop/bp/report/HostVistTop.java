package org.dueam.hadoop.bp.report;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.Value;
import org.dueam.report.common.XmlReportFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class HostVistTop {

    /**
     * @param args
     * @throws java.io.IOException
     */
    public static void main(String[] args) throws IOException {
        Report report = Report.newReport("单域名访问峰值统计");
        String input = args[0];
        if (!new File(input).exists()) {
            System.out.println("File Not Exist ! => " + input);
            return;
        }
        if (true) {
            final Map<String, Long> maxMap = new HashMap<String, Long>();
            final Map<String, String> maxTimeMap = new HashMap<String, String>();
            Utils.viewLines(input, new Utils.Callback() {
                public void call(String line) {
                    String[] _allCols = StringUtils.splitPreserveAllTokens(line, Utils.TAB);
                    String host = _allCols[0];
                    if (StringUtils.isEmpty(host) || "-".equals(host)) {
                        return;
                    }
                    long uv = NumberUtils.toLong(_allCols[3]);
                    String time = StringUtils.substring(_allCols[1], 8, 12);
                    if (StringUtils.length(time) > 3) {
                        time = time.substring(0, 2) + ":" + time.substring(2);
                    }
                    if (!maxMap.containsKey(host) || maxMap.get(host) < uv) {
                        maxMap.put(host, uv);
                        maxTimeMap.put(host, time);
                    }
                }
            });
            Table table = report.newTable("vist_uv_top", "每分钟单域名被访问次数（UV）");
            for (Map.Entry<String, Long> entry : maxMap.entrySet()) {
                if (entry.getValue() < 100) continue;
                final Value _tmpValue = Report.newValue(entry.getKey(), entry.getKey() + "[" + maxTimeMap.get(entry.getKey()) + "]", String.valueOf(entry.getValue()));
                table.addCol(_tmpValue);
                Utils.viewLines(input, new Utils.Callback() {
                    public void call(String line) {
                        String[] _allCols = StringUtils.splitPreserveAllTokens(line, Utils.TAB);
                        String host = _allCols[0];
                        if (StringUtils.isEmpty(host) || "-".equals(host) || !_tmpValue.getKey().equals(host)) {
                            return;
                        }
                        String pv = _allCols[3];
                        String time = StringUtils.substring(_allCols[1], 8, 12);
                        if (StringUtils.length(time) > 3) {
                            time = time.substring(0, 2) + ":" + time.substring(2);
                        }
                        _tmpValue.addDetail(time, pv);
                    }
                });
                _tmpValue.sortDetail(Value.SORT_TYPE_TIME);
            }
            table.sort(Table.SORT_VALUE);
        }
        if (true) {
            final Map<String, Long> maxMap = new HashMap<String, Long>();
            final Map<String, String> maxTimeMap = new HashMap<String, String>();
            Utils.viewLines(input, new Utils.Callback() {
                public void call(String line) {
                    String[] _allCols = StringUtils.splitPreserveAllTokens(line, Utils.TAB);
                    String host = _allCols[0];
                    if (StringUtils.isEmpty(host) || "-".equals(host)) {
                        return;
                    }
                    long pv = NumberUtils.toLong(_allCols[2]);
                    String time = StringUtils.substring(_allCols[1], 8, 12);
                    if (StringUtils.length(time) > 3) {
                        time = time.substring(0, 2) + ":" + time.substring(2);
                    }
                    if (!maxMap.containsKey(host) || maxMap.get(host) < pv) {
                        maxMap.put(host, pv);
                        maxTimeMap.put(host, time);
                    }
                }
            });
            Table table = report.newTable("vist_top", "每分钟单域名被访问次数（PV）");
            for (Map.Entry<String, Long> entry : maxMap.entrySet()) {
                if (entry.getValue() < 100) continue;
                final Value _tmpValue = Report.newValue(entry.getKey(), entry.getKey() + "[" + maxTimeMap.get(entry.getKey()) + "]", String.valueOf(entry.getValue()));
                table.addCol(_tmpValue);
                Utils.viewLines(input, new Utils.Callback() {
                    public void call(String line) {
                        String[] _allCols = StringUtils.splitPreserveAllTokens(line, Utils.TAB);
                        String host = _allCols[0];
                        if (StringUtils.isEmpty(host) || "-".equals(host) || !_tmpValue.getKey().equals(host)) {
                            return;
                        }
                        String pv = _allCols[2];
                        String time = StringUtils.substring(_allCols[1], 8, 12);
                        if (StringUtils.length(time) > 3) {
                            time = time.substring(0, 2) + ":" + time.substring(2);
                        }
                        _tmpValue.addDetail(time, pv);
                    }
                });

                _tmpValue.sortDetail(Value.SORT_TYPE_TIME);

            }
            table.sort(Table.SORT_VALUE);
        }
        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"), args[0], null);
    }


}
