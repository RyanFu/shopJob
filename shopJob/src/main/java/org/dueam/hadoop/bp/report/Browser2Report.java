package org.dueam.hadoop.bp.report;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.dueam.hadoop.common.util.Fmt;
import org.dueam.hadoop.common.util.MapUtils;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

/**
 * 通用转化率报表
 */
public class Browser2Report {
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

        String name = "";
        if (args.length > 1) {
            name = args[1];
        }

        Map<String, List<String[]>> today = MapUtils.map(Utils.read(input), CTRL_A);

        Map<String, String[]> ipvMap = MapUtils.toMap(today.get("ipv"));
        Map<String, String[]> tradeMap = MapUtils.toMap(today.get("trade"));

        Report report = Report.newReport(name + "转化率报表");

        Table trade = report.newViewTable("trade", "基本详情(交易相关)");

        trade.addCol("种类").addCol("支付宝金额").addCol("IPV-UV").addCol("购买UV").addCol("购买转化率").addCol("订单数").addCol("客单价")
                .addCol(Report.BREAK_VALUE);

        for (Entry<String, String[]> entry : ipvMap.entrySet()) {
            String key = entry.getKey();

            String[] ipv = entry.getValue();
            String[] tr = tradeMap.get(key);

            if (ipv == null) {
                ipv = new String[] { "0", "0", "0" };
            }

            if (tr == null) {
                tr = new String[] { "0", "0", "0", "0", "0" };
            }

            trade.addCol(key).addCol(tr[0]).addCol(ipv[1]).addCol(tr[2]).addCol(Fmt.parent2(tr[2], ipv[1])).addCol(
                    tr[4]).addCol(Fmt.div(tr[0], tr[2])).addCol(Report.BREAK_VALUE);

        }

        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
    }

}
