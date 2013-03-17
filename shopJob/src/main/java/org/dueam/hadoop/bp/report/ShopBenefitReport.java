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
 * �����Ż�ȯ����
 */
public class ShopBenefitReport {
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

        Report report = Report.newReport("���е����Ż�ȯ�ձ�");

        Table summay = report.newTable("summay", "��������");

        Map<String, Table> tableMap = new HashMap<String, Table>();

        long sendCount = 0;
        long sendMoney = 0;

        long usedCount = 0;
        long usedMoney = 0;

        for (String line : Utils.readWithCharset(input, "utf-8")) {
            String[] allCols = StringUtils.splitPreserveAllTokens(line, CTRL_A);

            Table table = tableMap.get(allCols[1]);

            if (table == null) {
                table = report.newTable(allCols[1], allCols[1]);
                tableMap.put(allCols[1], table);
            }

            if ("all".equals(allCols[0])) {

                sendCount += Fmt.parseLong(allCols[2]);
                sendMoney += Fmt.parseLong(allCols[3]);

                table.addCol("��������", allCols[2]);
                table.addCol("���ͽ��", String.valueOf(sendMoney / 100));

            } else if ("used".equals(allCols[0])) {

                usedCount += Fmt.parseLong(allCols[2]);
                usedMoney += Fmt.parseLong(allCols[3]);

                table.addCol("��������", allCols[2]);
                table.addCol("���Ľ��", String.valueOf(usedMoney / 100));

            }
        }

        summay.addCol("�ܷ�������", String.valueOf(sendCount));
        summay.addCol("�ܷ��ͽ��", String.valueOf(sendMoney / 100));
        summay.addCol("����������", String.valueOf(usedCount));
        summay.addCol("�����Ľ��", String.valueOf(usedMoney / 100));

        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
    }
}
