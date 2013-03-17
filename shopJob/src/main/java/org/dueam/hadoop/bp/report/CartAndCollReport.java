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
 * ȫ�����ﳵ���ղر���
 */
public class CartAndCollReport {
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

        Report report = Report.newReport("ȫ�����ﳵ���ղ��ձ�");

        Table summay = report.newTable("summay", "��������");

        Map<String, Table> tableMap = new HashMap<String, Table>();

        tableMap.put("member_cart", report.newTable("member_cart", "��Ա�¼ӹ��ﳵ����(��һ����Ŀ�ֲ�)"));
        tableMap.put("guest_cart", report.newTable("guest_cart", "�ÿ��¼ӹ��ﳵ����(��һ����Ŀ�ֲ�)"));

        tableMap.put("collect_item", report.newTable("collect_item", "���ղر�������(��һ����Ŀ�ֲ�)"));
        tableMap.put("collect_shop", report.newTable("collect_shop", "���ղص�������(����Ӫ��Ŀ�ֲ�)"));

        for (String line : Utils.readWithCharset(input, "utf-8")) {
            String[] allCols = StringUtils.splitPreserveAllTokens(line, CTRL_A);

            if ("member_cart".equals(allCols[0])) {
                if ("all".equals(allCols[1])) {
                    summay.addCol("�¼��빺�ﳵ������(��Ա)", allCols[2]);
                } else {
                    tableMap.get("member_cart").addCol(allCols[1], allCols[2]);
                }
            }

            if ("collect_all".equals(allCols[0])) {

                if ("1".equals(allCols[1])) {
                    summay.addCol("���ղر�������", allCols[2]);
                } else if ("0".equals(allCols[1])) {
                    summay.addCol("���ղص�������", allCols[2]);
                }
            }

            if ("collect_item".equals(allCols[0])) {
                tableMap.get("collect_item").addCol(allCols[1], allCols[2]);
            }

            if ("collect_shop".equals(allCols[0])) {
                tableMap.get("collect_shop").addCol(allCols[1], allCols[2]);
            }

            if ("guest_cart".equals(allCols[0])) {
                if ("all".equals(allCols[1])) {
                    summay.addCol("�¼��빺�ﳵ������(�ÿ�)", allCols[2]);
                } else {
                    tableMap.get("guest_cart").addCol(allCols[1], allCols[2]);
                }
            }
        }

        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
    }
}
