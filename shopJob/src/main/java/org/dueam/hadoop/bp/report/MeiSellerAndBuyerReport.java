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
 * 买家,卖家通用报表
 */
public class MeiSellerAndBuyerReport {
    private static char CTRL_A = (char) 0x01;

    private static Map<String, String> vipMap = new HashMap<String, String>();

    static {

        vipMap.put("-2", "(-2)普通会员");
        vipMap.put("-1", "(-1)普通会员");
        vipMap.put("0", "(0)准vip会员");
        vipMap.put("1", "(1)黄金会员");
        vipMap.put("2", "(2)白金会员");
        vipMap.put("3", "(3)钻石会员");
        vipMap.put("4", "(4)VIP会员");
        vipMap.put("10", "(10)淘宝达人");

    }

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

        String name = "";
        if (StringUtils.isNotBlank(args[1])) {
            name = args[1];
        }

        Report report = Report.newReport(name + "卖家 ,买家报表");

        Map<String, Table> allTable = new HashMap<String, Table>();

        allTable.put("seller_star", report.newTable("seller_star", "卖家等级分布"));
        allTable.put("seller_trade", report.newTable("seller_trade", "卖家交易额分布"));
        allTable.put("buyer_star", report.newTable("buyer_star", "买家等级分布"));
        allTable.put("buyer_vip", report.newTable("buyer_vip", "买家VIP分布"));
        allTable.put("buyer_trade", report.newTable("buyer_trade", "买家交易额分布"));

        for (String line : Utils.readWithCharset(input, "utf-8")) {
            String[] allCols = StringUtils.splitPreserveAllTokens(line, CTRL_A);

            Table tmp = allTable.get(allCols[0]);
            if (tmp != null) {
                if ("buyer_vip".equals(allCols[0])) {
                    tmp.addCol(vipMap.get(allCols[1]) != null ? vipMap.get(allCols[1]) : "other", allCols[2]);
                } else {
                    tmp.addCol(allCols[1], allCols[2]);
                }
            }
        }
        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
    }
}
