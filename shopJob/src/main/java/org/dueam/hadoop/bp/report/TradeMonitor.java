package org.dueam.hadoop.bp.report;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.dueam.hadoop.common.Area;
import org.dueam.hadoop.common.util.ItemUtils;
import org.dueam.hadoop.common.util.MapUtils;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.hadoop.services.Category;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 交易监控报表
 */
public class TradeMonitor {
    static Map<String, String> keyMap = Utils.asLinkedMap(
            "created",
            "创建记录数",
            "modified",
            "修改记录数",
            "gmv",
            "GMV",
            "gmv_num",
            "GMV笔数",
            "alipay",
            "支付宝交易额",
            "alipay_num",
            "支付宝笔数");

    /**
     * @param args
     * @throws java.io.IOException
     */
    public static void main(String[] args) throws IOException {
        Report report = Report.newReport("交易监控报表");
        String input = args[0];
        if (!new File(input).exists()) {
            System.out.println("File Not Exist ! => " + input);
            return;
        }
        Map<String, List<String[]>> today = MapUtils.map(Utils.read(input, new String[]{"d"}));
        today = MapUtils.map(today.get("t"));
        if (today == null) {
            System.out.println("No Data ! => " + input);
            return;
        }
        if (true && today.containsKey("sys")) {
            Map<String, String[]> statusMap = MapUtils.toMap(today.get("sys"));
            if (true) {
                Table table = report.newTable("sys", "交易基本系统信息");
                for (String key : statusMap.keySet()) {
                    table.addCol(key, keyMap.get(key), statusMap.get(key)[0]);
                }
            }

        }
        for (String[] keyValues : new String[][]{{"all", "全网交易基本信息（单位：元）"}, {"b2c", "商城（B2C）交易基本信息（单位：元）"}, {"c2c", "集市（C2C）交易基本信息（单位：元）"}, {"jhs", "聚划算交易基本信息（单位：元）"}}) {
            String groupKey = keyValues[0];
            String groupName = keyValues[1];
            if (true && today.containsKey(groupKey)) {
                Map<String, String[]> statusMap = MapUtils.toMap(today.get(groupKey));
                if (true) {
                    Table table = report.newTable(groupKey, groupName);
                    for (String key : statusMap.keySet()) {
                        long value = NumberUtils.toLong(statusMap.get(key)[0], 0);
                        if ("gmv".equals(key) || "alipay".equals(key)) {
                            value = value / 100;
                        }
                        table.addCol("t_" + key, keyMap.get(key), String.valueOf(value));
                        if (true) {
                            String line = Utils.getMax(input, Utils.merge("d", groupKey, "m", key), 5, Utils.TAB);
                            String[] _cols = StringUtils.splitPreserveAllTokens(line, Utils.TAB);
                            if (_cols.length > 5) {
                                long _colValue = NumberUtils.toLong(_cols[5]);
                                if ("gmv".equals(key) || "alipay".equals(key)) {
                                    _colValue = _colValue / 100;
                                }
                                table.addCol("t_" + key + "_m_max", keyMap.get(key) + "每分钟峰值(" + _cols[4] + ")", String.valueOf(_colValue));
                            }
                        }

                        if (true) {
                            String line = Utils.getMax(input, Utils.merge("d", groupKey, "s", key), 5, Utils.TAB);
                            String[] _cols = StringUtils.splitPreserveAllTokens(line, Utils.TAB);
                            if (_cols.length > 5) {
                                long _colValue = NumberUtils.toLong(_cols[5]);
                                if ("gmv".equals(key) || "alipay".equals(key)) {
                                    _colValue = _colValue / 100;
                                }
                                table.addCol("t_" + key + "_s_max", keyMap.get(key) + "每秒钟峰值(" + _cols[4] + ")", String.valueOf(_colValue));
                            }
                        }
                    }
                    //table.sort(Table.SORT_KEY);
                }
            }
        }

        if (true && today.containsKey("alipay_area")) {
            Map<String, String[]> statusMap = MapUtils.toMap(today.get("alipay_area"));
            if (true) {
                Table table = report.newGroupTable("alipay_area", "订单金额分布（单位：元）");
                for (String key : alipayTradeArea.areaKeys) {
                    if(statusMap.containsKey(key)){
                        table.addCol(key, key, statusMap.get(key)[0]);
                    }
                }
            }

        }
        if (true && today.containsKey("cat")) {
            Map<String, List<String[]>> statusMap = MapUtils.map(today.get("cat"));
            if (true) {
                Table table = report.newGroupTable("cat_alipay", "分类目支付宝交易金额（单位：元）");
                Map<String, String[]> subStatusMap = MapUtils.toMap(statusMap.get("alipay"));
                for (String key : subStatusMap.keySet()) {
                    table.addCol(key, Category.getCategoryName(key), Utils.toYuan(subStatusMap.get(key)[0]));
                }
                table.sort(Table.SORT_VALUE);
            }

            if (true) {
                Table table = report.newGroupTable("cat_alipay_num", "分类目支付宝交易笔数（子订单）");
                Map<String, String[]> subStatusMap = MapUtils.toMap(statusMap.get("alipay_num"));
                for (String key : subStatusMap.keySet()) {
                    table.addCol(key, Category.getCategoryName(key), subStatusMap.get(key)[0]);
                }
                table.sort(Table.SORT_VALUE);
            }

        }
        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
    }
    static Area alipayTradeArea = Area.newArea(Area.MoneyCallback, 0, 1 * 100, 10 * 100, 50 * 100, 100 * 100, 200 * 100, 300 * 100, 500 * 100, 10000 * 100, 100000 * 100);
    static String sum(Map<String, String> statusMap, String key) {
        long sum = 0;
        for (String status : ItemUtils.allStatus) {
            String newKey = key + "^" + status;
            sum += NumberUtils.toLong(statusMap.get(newKey) + "", 0);

        }
        if (sum <= 0) return "0";
        return String.valueOf(sum);
    }

    @SuppressWarnings("unchecked")
    static Map<String, String> getMap(String date) {
        try {
            Map<String, String> statusMap = new HashMap<String, String>();
            for (String line : (List<String>) FileUtils.readLines(new File(date), "GBK")) {
                String[] _array = StringUtils.split(line, "\t");
                String gmtDate = _array[0];
                String status = _array[1];
                String count = _array[2];
                statusMap.put(gmtDate + "^" + status, count);
            }
            return statusMap;
        } catch (Exception e) {
        }
        return null;
    }
}
