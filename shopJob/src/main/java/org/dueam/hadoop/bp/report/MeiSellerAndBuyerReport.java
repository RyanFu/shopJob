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
 * ���,����ͨ�ñ���
 */
public class MeiSellerAndBuyerReport {
    private static char CTRL_A = (char) 0x01;

    private static Map<String, String> vipMap = new HashMap<String, String>();

    static {

        vipMap.put("-2", "(-2)��ͨ��Ա");
        vipMap.put("-1", "(-1)��ͨ��Ա");
        vipMap.put("0", "(0)׼vip��Ա");
        vipMap.put("1", "(1)�ƽ��Ա");
        vipMap.put("2", "(2)�׽��Ա");
        vipMap.put("3", "(3)��ʯ��Ա");
        vipMap.put("4", "(4)VIP��Ա");
        vipMap.put("10", "(10)�Ա�����");

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

        Report report = Report.newReport(name + "���� ,��ұ���");

        Map<String, Table> allTable = new HashMap<String, Table>();

        allTable.put("seller_star", report.newTable("seller_star", "���ҵȼ��ֲ�"));
        allTable.put("seller_trade", report.newTable("seller_trade", "���ҽ��׶�ֲ�"));
        allTable.put("buyer_star", report.newTable("buyer_star", "��ҵȼ��ֲ�"));
        allTable.put("buyer_vip", report.newTable("buyer_vip", "���VIP�ֲ�"));
        allTable.put("buyer_trade", report.newTable("buyer_trade", "��ҽ��׶�ֲ�"));

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
