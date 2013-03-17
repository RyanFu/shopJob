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
 * ˫11��Ҹ��ٱ���
 * linyi
 */
public class D11BuyerReport {
    private static char CTRL_A = (char) 0x01;

    private static Map<String, String> buyerMap = new HashMap<String, String>();

    private static Map<String, String> typeMap = new HashMap<String, String>();

    static {

        buyerMap.put("b", "˫11���̳����");
        buyerMap.put("c", "˫11���������");
        buyerMap.put("bc", "˫11������");

        typeMap.put("b", "�̳�");
        typeMap.put("c", "����");

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

        Report report = Report.newReport("˫11��Ҹ��ٱ���");

        Table total = report.newViewTable("total", "��������");

        total.addCol("�������").addCol("��������").addCol("���").addCol("������").addCol("����").addCol(Report.BREAK_VALUE);

        Table detail = report.newTable("detail", "�������");

        for (String line : Utils.readWithCharset(input, "utf-8")) {
            String[] allCols = StringUtils.splitPreserveAllTokens(line, CTRL_A);

            total.addCol(buyerMap.get(allCols[0])).addCol(typeMap.get(allCols[1])).addCol(allCols[2])
                    .addCol(allCols[3]).addCol(Fmt.div(allCols[2], allCols[3])).addCol(Report.BREAK_VALUE);

            detail.addCol(buyerMap.get(allCols[0]) + "(" + typeMap.get(allCols[1]) + ")", allCols[2]);

        }

        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
    }
}
