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
 * �����Ź�ͳ��
 */
public class TuanReport {
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

        Report report = Report.newReport("�����Ź�����");

        Table total = report.newViewTable("total", "�����Ź� ��������");
        Table detail = report.newTable("detail", "�����Ź�  �������");
        total.addCol("����").addCol("PV").addCol("UV����Ա��").addCol("�ۻ���PV").addCol("�ۻ���UV").addCol("���ת����").addCol(
                "PV/��").addCol(Report.BREAK_VALUE);

        Map<String, String[]> lineMap = getLineMap(input);

        String[] ls = lineMap.get("ls");

        String[] ipv = lineMap.get("ipv");

        detail.addCol("�����Ź�PV", ls[1]);
        detail.addCol("�����Ź�UV����Ա��", ls[2]);
        detail.addCol("�����Ź�UV��MID��", ls[3]);

        detail.addCol("�ۻ���PV", ipv[1]);
        detail.addCol("�ۻ���UV����Ա��", ipv[2]);
        detail.addCol("�ۻ���UV��MID��", ipv[3]);
        detail.addCol("���ת����(��ֱ�)", Fmt.parent3(ipv[2], ls[2]));

        total.addCol("�����Ź�").addCol(ls[1]).addCol(ls[2]).addCol(ipv[1]).addCol(ipv[2]).addCol(
                Fmt.parent2(ipv[2], ls[2])).addCol(Fmt.div(ls[1], ls[3])).addCol(Report.BREAK_VALUE);

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
