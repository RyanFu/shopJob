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
public class BrowserReport {
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

        Map<String, String[]> lsMap = MapUtils.toMap(today.get("ls"));
        Map<String, String[]> ipvMap = MapUtils.toMap(today.get("ipv"));
        Map<String, String[]> tradeMap = MapUtils.toMap(today.get("trade"));

        Report report = Report.newReport(name + "转化率报表");

        Table total = report.newViewTable("total", "基本详情");

        Table trade = report.newViewTable("trade", "基本详情(交易相关)");

        total.addCol("种类").addCol("PV").addCol("UV（会员）").addCol("IPV").addCol("IPV-UV").addCol("浏览转化率").addCol("PV/人")
                .addCol(Report.BREAK_VALUE);

        trade.addCol("种类").addCol("支付宝金额").addCol("IPV-UV").addCol("购买UV").addCol("购买转化率").addCol("订单数").addCol("客单价")
                .addCol(Report.BREAK_VALUE);

        for (Entry<String, String[]> entry : lsMap.entrySet()) {
            String key = entry.getKey();

            String[] ls = entry.getValue();
            String[] ipv = ipvMap.get(key);
            String[] tr = tradeMap.get(key);

            if (ipv == null) {
                ipv = new String[] { "0", "0", "0" };
            }

            if (tr == null) {
                tr = new String[] { "0", "0", "0", "0", "0" };
            }

            total.addCol(key).addCol(ls[0]).addCol(ls[1]).addCol(ipv[0]).addCol(ipv[1]).addCol(
                    Fmt.parent2(ipv[1], ls[1])).addCol(Fmt.div(ls[0], ls[2])).addCol(Report.BREAK_VALUE);

            trade.addCol(key).addCol(tr[0]).addCol(ipv[1]).addCol(tr[2]).addCol(Fmt.parent2(tr[2], ipv[1])).addCol(
                    tr[4]).addCol(Fmt.div(tr[0], tr[2])).addCol(Report.BREAK_VALUE);

            Table detail = report.newTable(key + "detail", key + "浏览详情");

            detail.addCol("PV", ls[0]);
            detail.addCol("UV（会员）", ls[1]);
            detail.addCol("UV（MID）", ls[2]);

            detail.addCol("IPV", ipv[0]);
            detail.addCol("IPV-UV（会员）", ipv[1]);
            detail.addCol("IPV-UV（MID）", ipv[2]);

            detail.addCol("浏览转化率(万分比)", Fmt.parent3(ipv[1], ls[1]));

            detail.addCol("支付宝金额", tr[0]);
            detail.addCol("购买UV", tr[2]);
            detail.addCol("订单数", tr[4]);

        }

        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
    }

}
