package org.dueam.hadoop.bp.report;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
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

public class SecondNewItemDailyFrom {


    /**
     * @param args
     * @throws java.io.IOException
     */
    public static void main(String[] args) throws IOException {
        String input = args[0];
        if (!new File(input).exists()) {
            return;
        }
        Map<String, Long> statusMap = getMap( input);
        if (statusMap == null) return;
        Report report = Report.newReport("二手新发布商品来源跟踪");
        if (true) {
            Table table = report.newGroupTable("all", "商品发布来源分布", "来源", "数量");

            for (String key : SOURCE) {
                table.addCol(key, sum(statusMap, key));

            }
        }

        if (true) {
            for (String key : SOURCE) {
                Table table = report.newGroupTable("lite_" + key, key + "发布来源分布", "状态", "数量");
                for (String status : ItemUtils.allStatus) {
                    Long _num = statusMap.get(key + "^" + status);
                    if (_num == null) {
                        _num = 0L;
                    }
                    table.addCol(status, ItemUtils.getItemStatusName(status), String.valueOf(_num));
                }
            }
        }

        if (true) {
            Map<String, List<String[]>> today = MapUtils.map(Utils.read(input, (String[])null));
            if (true && today.containsKey("cat")) {
                Map<String, Long> _countMap = new HashMap<String, Long>();
                for (String[] array : today.get("cat")) {
                    String catId = array[0];
                    if (null != Category.getCategory(catId)) {
                        catId = Category.getCategory(catId).getRootId();
                    }
                    long value = NumberUtils.toLong(array[1]);
                    Long _v = _countMap.get(catId);
                    if (_v == null) {
                        _v = 0L;
                    }
                    _v = _v + value;
                    _countMap.put(catId, _v);
                }
                Table table = report.newGroupTable("cat", "今天发布商品的各个类目分布");
                for (Map.Entry<String, Long> entry : _countMap.entrySet()) {
                    table.addCol(entry.getKey(), Category.getCategoryName(entry.getKey()), String.valueOf(entry.getValue()));
                }
                table.sort(Table.SORT_VALUE);

            }
        }
        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));

    }

    final static String[] SOURCE = new String[]{"TOP", "SELL", "DPC", "TBCP", "NULL"};

    static String sum(Map<String, Long> yesday, String key) {
        long sum = 0;
        for (String status : ItemUtils.allStatus) {
            String newKey = key + "^" + status;
            sum += NumberUtils.toLong(yesday.get(newKey) + "", 0);

        }
        if (sum <= 0) return "0";
        return String.valueOf(sum);
    }

    @SuppressWarnings("unchecked")
    static Map<String, Long/* array[0]=count,array[1]=sum */> getMap(String date) {
        if (!new File(date).exists())
            return null;
        try {

            Map<String, Long> statusMap = new HashMap<String, Long>();

            for (String line : (List<String>) FileUtils.readLines(new File(date), "GBK")) {
                String[] _array = StringUtils.split(line, "\t");
                String key = (_array[0] + "^" + _array[1]).toUpperCase();
                if (statusMap.containsKey(key)) {
                    statusMap.put(key, statusMap.get(key) + NumberUtils.toLong(_array[2]));
                } else {
                    statusMap.put(key, NumberUtils.toLong(_array[2]));
                }
            }
            return statusMap;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
