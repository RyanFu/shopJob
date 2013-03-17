package org.dueam.hadoop.bp.report;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.dueam.hadoop.common.util.Fmt;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

/**
 * ��ױ��Ʒ����
 */
public class MeiItemReport {
    private static char CTRL_A = (char) 0x01;

    private static Map<String, String> catMap = new HashMap<String, String>();

    static {

        catMap.put("1801", "���ݻ���/����/����");
        catMap.put("50010788", "��ױ/��ˮ/��ױ����");
        catMap.put("50023282", "��������/�ٷ�");

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

        Report report = Report.newReport("��ױ�г���Ʒ���ױ���");

        Map<String, Table> summayMap = new HashMap<String, Table>();

        for (Entry<String, String> entry : catMap.entrySet()) {
            Table summay = report.newTable(entry.getKey() + "summay", entry.getValue() + "--��������");
            summayMap.put(entry.getKey(), summay);
        }

        for (String line : Utils.readWithCharset(input, "utf-8")) {
            String[] allCols = StringUtils.splitPreserveAllTokens(line, CTRL_A);

            if ("summay".equals(allCols[0])) {

                Table summay = summayMap.get(allCols[1]);
                if (summay == null) {
                    continue;
                }

                summay.addCol("��Ʒ��", allCols[2]);
                summay.addCol("������Ʒ��", allCols[3]);
                summay.addCol("�·���Ʒ��", allCols[4]);
                summay.addCol("������Ʒ��", allCols[5]);
                continue;
            }

            if ("ipv".equals(allCols[0])) {

                Table summay = summayMap.get(allCols[1]);
                if (summay == null) {
                    continue;
                }

                summay.addCol("IPV", allCols[2]);
                summay.addCol("IPV-UV", allCols[3]);
                continue;
            }

            if ("trade".equals(allCols[0])) {

                Table summay = summayMap.get(allCols[1]);
                if (summay == null) {
                    continue;
                }

                summay.addCol("֧�������", StringUtils.isNumeric(allCols[2]) ? allCols[2] : "0");
                summay.addCol("�����������Ʒ��", allCols[3]);
                summay.addCol("��������������", allCols[4]);
                summay.addCol("���������������", allCols[5]);
                summay.addCol("������", allCols[6]);
                summay.addCol("�͵���", Fmt.parent4(allCols[2], allCols[4]));
                continue;
            }

            if ("memeber_cart".equals(allCols[0])) {

                Table summay = summayMap.get(allCols[1]);
                if (summay == null) {
                    continue;
                }

                summay.addCol("���빺�ﳵ�ܴ���(��Ա)", allCols[2]);
                continue;
            }

            if ("guest_cart".equals(allCols[0])) {

                Table summay = summayMap.get(allCols[1]);
                if (summay == null) {
                    continue;
                }
                summay.addCol("���빺�ﳵ�ܴ���(�ÿ�)", allCols[2]);
                continue;
            }

        }

        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
    }
}
