package org.dueam.hadoop.bp.report;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

/**
 * 全网购物车和收藏报表
 */
public class CartAndCollReport {
    private static char CTRL_A = (char) 0x01;

    /**
     * @param args
     * @throws java.io.IOException
     */
    public static void main(String[] args) throws IOException {
        String input = args[0];
        if (!new File(input).exists()) {
            System.out.println("File Not Exist ! => " + input);
            return;
        }

        Report report = Report.newReport("全网购物车和收藏日报");

        Table summay = report.newTable("summay", "基本详情");

        Map<String, Table> tableMap = new HashMap<String, Table>();

        tableMap.put("member_cart", report.newTable("member_cart", "会员新加购物车人数(按一级类目分布)"));
        tableMap.put("guest_cart", report.newTable("guest_cart", "访客新加购物车人数(按一级类目分布)"));

        tableMap.put("collect_item", report.newTable("collect_item", "新收藏宝贝人数(按一级类目分布)"));
        tableMap.put("collect_shop", report.newTable("collect_shop", "新收藏店铺人数(按主营类目分布)"));

        for (String line : Utils.readWithCharset(input, "utf-8")) {
            String[] allCols = StringUtils.splitPreserveAllTokens(line, CTRL_A);

            if ("member_cart".equals(allCols[0])) {
                if ("all".equals(allCols[1])) {
                    summay.addCol("新加入购物车总人数(会员)", allCols[2]);
                } else {
                    tableMap.get("member_cart").addCol(allCols[1], allCols[2]);
                }
            }

            if ("collect_all".equals(allCols[0])) {

                if ("1".equals(allCols[1])) {
                    summay.addCol("新收藏宝贝人数", allCols[2]);
                } else if ("0".equals(allCols[1])) {
                    summay.addCol("新收藏店铺人数", allCols[2]);
                }
            }

            if ("collect_item".equals(allCols[0])) {
                tableMap.get("collect_item").addCol(allCols[1], allCols[2]);
            }

            if ("collect_shop".equals(allCols[0])) {
                tableMap.get("collect_shop").addCol(allCols[1], allCols[2]);
            }

            if ("guest_cart".equals(allCols[0])) {
                if ("all".equals(allCols[1])) {
                    summay.addCol("新加入购物车总人数(访客)", allCols[2]);
                } else {
                    tableMap.get("guest_cart").addCol(allCols[1], allCols[2]);
                }
            }
        }

        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
    }
}
