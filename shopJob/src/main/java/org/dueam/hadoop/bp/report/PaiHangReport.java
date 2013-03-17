package org.dueam.hadoop.bp.report;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.dueam.hadoop.common.util.Fmt;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

/**
 * �������а�ͳ��
 */
public class PaiHangReport {
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

        Report report = Report.newReport("���а񱨱�");

        Table total = report.newViewTable("total", "���а� ��������");

        Table trade = report.newViewTable("trade", "���а� ��������(�������)");

        Table detail = report.newTable("detail", "���а�  �������");

        total.addCol("����").addCol("PV").addCol("UV����Ա��").addCol("IPV").addCol("IPV-UV").addCol("���ת����").addCol("PV/��")
                .addCol(Report.BREAK_VALUE);

        trade.addCol("����").addCol("֧�������").addCol("IPV-UV").addCol("����UV").addCol("����ת����").addCol("������").addCol("�͵���")
                .addCol(Report.BREAK_VALUE);

        Map<String, String[]> lineMap = getLineMap(input);

        String[] ls = lineMap.get("ls");

        String[] ipv = lineMap.get("ipv");

        String[] tr = lineMap.get("trade");

        detail.addCol("���а�PV", ls[1]);
        detail.addCol("���а�UV����Ա��", ls[2]);
        detail.addCol("���а�UV��MID��", ls[3]);

        detail.addCol("IPV", ipv[1]);
        detail.addCol("IPV-UV����Ա��", ipv[2]);
        detail.addCol("IPV-UV��MID��", ipv[3]);

        detail.addCol("֧�������", tr[1]);
        detail.addCol("����UV", tr[3]);
        detail.addCol("������", tr[4]);

        total.addCol("���а�").addCol(ls[1]).addCol(ls[2]).addCol(ipv[1]).addCol(ipv[2])
                .addCol(Fmt.parent2(ipv[2], ls[2])).addCol(Fmt.div(ls[1], ls[3])).addCol(Report.BREAK_VALUE);

        trade.addCol("���а�").addCol(tr[1]).addCol(ipv[2]).addCol(tr[3]).addCol(Fmt.parent2(tr[3], ipv[2])).addCol(tr[4])
                .addCol(Fmt.div(tr[1], tr[3])).addCol(Report.BREAK_VALUE);

        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
    }

    private static Map<String, String[]> getLineMap(String input) throws IOException {

        Map<String, String[]> lineMap = new HashMap<String, String[]>();

        for (String line : Utils.readWithCharset(input, "utf-8")) {
            String[] _allCols = StringUtils.splitPreserveAllTokens(line, CTRL_A);
            lineMap.put(_allCols[0], _allCols);
        }

        return lineMap;

    }

}
