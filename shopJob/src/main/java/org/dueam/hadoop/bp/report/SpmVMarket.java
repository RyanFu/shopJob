package org.dueam.hadoop.bp.report;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.dueam.hadoop.common.CounterMap;
import org.dueam.hadoop.common.util.MapUtils;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.hadoop.services.ShopDomain;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.commons.lang.math.NumberUtils.toInt;
import static org.apache.commons.lang.math.NumberUtils.toLong;
import static org.dueam.hadoop.common.Functions.str;

/**
 * 风格馆页面点击统计
 */
public class SpmVMarket {
    private static char CTRL_A = (char) 0x01;

    /**
     * @param args
     * @throws java.io.IOException
     */
    public static void main(String[] args) throws IOException {
        String input = args[0];
        //String input = "20110923";
        if (!new File(input).exists()) {
            System.out.println("File Not Exist ! => " + input);
            return;
        }

        Report report = Report.newReport("风格馆&特色馆页面点击统计");

        Map<String, List<String[]>> today = MapUtils.map(Utils.read(input, (String[]) null), CTRL_A);
        if (today.containsKey("module")) {
            Table table = report.newViewTable("total", "各个模块点击统计");
            table.addCol("模块名称").addCol("PV").addCol("UPV").addCol("UV").addCol("UUV").breakRow();
            Map<String, String[]> statusMap = MapUtils.toMap(today.get("module"));
            for (Map.Entry<String, String> entry : KEYS.entrySet()) {
                if (statusMap.containsKey(entry.getKey())) {
                    String[] _cols = statusMap.get(entry.getKey());
                    table.addCol(entry.getValue());
                    table.addCol(entry.getKey() + "-pv", str(toLong(_cols[0])));
                    table.addCol(entry.getKey() + "-upv", str(toLong(_cols[1])));
                    table.addCol(entry.getKey() + "-uv", str(toLong(_cols[2])));
                    table.addCol(entry.getKey() + "-uuv", str(toLong(_cols[3])));
                    table.breakRow();
                }
            }
        }
        if (today.containsKey("position")) {
            Map<String, List<String[]>> statusMap = MapUtils.map(today.get("position"));

            for (Map.Entry<String, String> entry : KEYS.entrySet()) {
                if (statusMap.containsKey(entry.getKey())) {
                    Table table = report.newViewTable("detail_" + entry.getKey(), entry.getValue() + "模块点击详情");
                    table.addCol("点击位置").addCol("PV").addCol("UPV").addCol("UV").addCol("UUV").breakRow();
                    List<String[]> _colsList = statusMap.get(entry.getKey());
                    Collections.sort(_colsList, new Comparator<String[]>() {
                        public int compare(String[] o1, String[] o2) {
                            return toInt(o1[0]) - toInt(o2[0]);
                        }
                    });

                    for (String[] _cols : _colsList) {
                        String key = entry.getKey() + "." + _cols[0];
                        table.addCol(key);
                        table.addCol(key + "-pv", str(toLong(_cols[1])));
                        table.addCol(key + "-upv", str(toLong(_cols[2])));
                        table.addCol(key + "-uv", str(toLong(_cols[3])));
                        table.addCol(key + "-uuv", str(toLong(_cols[4])));
                        table.breakRow();
                    }
                }
            }
        }

        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
    }

    static Map<String, String> KEYS = MapUtils.asMap(("10\n" +
            "市场切换\n" +
            "11\n" +
            "女装属性选择栏的头部\n" +
            "12\n" +
            "属性显示区\n" +
            "13\n" +
            "排序栏\n" +
            "14\n" +
            "地区选择\n" +
            "15\n" +
            "商品列表的图片点击\n" +
            "16\n" +
            "进入店铺点击\n" +
            "17\n" +
            "进入评价点击\n" +
            "18\n" +
            "页底翻页统计\n" +
            "19\n" +
            "快速浏览模式\n" +
            "20\n" +
            "宝贝视图的异步请求\n" +
            "22\n" +
            "页面收起，展开，回顶部\n" +
            "23\n" +
            "快速浏览模式\n" +
            "24\n" +
            "搭配点击").split("\n"));
}
