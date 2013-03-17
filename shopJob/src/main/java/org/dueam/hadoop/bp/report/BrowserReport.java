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
 * ͨ��ת���ʱ���
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

        Report report = Report.newReport(name + "ת���ʱ���");

        Table total = report.newViewTable("total", "��������");

        Table trade = report.newViewTable("trade", "��������(�������)");

        total.addCol("����").addCol("PV").addCol("UV����Ա��").addCol("IPV").addCol("IPV-UV").addCol("���ת����").addCol("PV/��")
                .addCol(Report.BREAK_VALUE);

        trade.addCol("����").addCol("֧�������").addCol("IPV-UV").addCol("����UV").addCol("����ת����").addCol("������").addCol("�͵���")
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

            Table detail = report.newTable(key + "detail", key + "�������");

            detail.addCol("PV", ls[0]);
            detail.addCol("UV����Ա��", ls[1]);
            detail.addCol("UV��MID��", ls[2]);

            detail.addCol("IPV", ipv[0]);
            detail.addCol("IPV-UV����Ա��", ipv[1]);
            detail.addCol("IPV-UV��MID��", ipv[2]);

            detail.addCol("���ת����(��ֱ�)", Fmt.parent3(ipv[1], ls[1]));

            detail.addCol("֧�������", tr[0]);
            detail.addCol("����UV", tr[2]);
            detail.addCol("������", tr[4]);

        }

        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
    }

}
