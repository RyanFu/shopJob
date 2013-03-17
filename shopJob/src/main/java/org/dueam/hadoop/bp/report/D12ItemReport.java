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
 * ˫12��Ʒ���ٱ���
 */
public class D12ItemReport {
    private static char CTRL_A = (char) 0x01;

    private static Map<String, String> auctionStatusMap = new HashMap<String, String>();

    static {

        auctionStatusMap.put("0", "����");
        auctionStatusMap.put("1", "С��ȷ��");
        auctionStatusMap.put("-1", "�û�ɾ��");
        auctionStatusMap.put("-2", "�û��¼�");
        auctionStatusMap.put("-3", "С���¼�");
        auctionStatusMap.put("-4", "С��ɾ��");
        auctionStatusMap.put("-5", "��δ�ϼ�");
        auctionStatusMap.put("-9", "CC");
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

        Report report = Report.newReport("˫12��Ʒ���ٱ���");

        Table summay = report.newTable("summay", "��������");

        Map<String, Table> tableMap = new HashMap<String, Table>();

        tableMap.put("zk_type", report.newTable("zk_type", "�ۿ۷ֲ�"));
        tableMap.put("zk_time", report.newTable("zk_time", "�ۿ�ʱ��ֲ�"));
        tableMap.put("quantity_type", report.newTable("quantity_type", "���ֲ�"));
        tableMap.put("auction_status", report.newTable("auction_status", "״̬�ֲ�"));
        tableMap.put("category", report.newTable("category", "һ����Ŀ�ֲ�"));

        Table errorItem = report.newViewTable("error_item", "������Ʒ��Ϣ(�ۿ�С��2��,���ڵ���10��)");
        errorItem.addCol("��Ʒid").addCol("һ����Ŀ").addCol("����").addCol("�ۿ�(ǧ�ֱ�)").addCol("���Ƽ�(��)").addCol(
                Report.BREAK_VALUE);

        for (String line : Utils.readWithCharset(input, "utf-8")) {
            String[] allCols = StringUtils.splitPreserveAllTokens(line, CTRL_A);

            if ("summay".equals(allCols[0])) {
                summay.addCol("��Ʒ��", allCols[1]);
                summay.addCol("�ܿ��", allCols[2]);
                summay.addCol("ƽ�����", Fmt.parent4(allCols[2], allCols[1]));
                summay.addCol("������Ʒ��", allCols[3]);
                summay.addCol("һ�ڼ���Ʒ��", allCols[4]);
                summay.addCol("�̳���Ʒ��", allCols[5]);
                summay.addCol("���ղ���", allCols[6]);
                summay.addCol("ƽ���ղ���", Fmt.parent4(allCols[6], allCols[1]));
                summay.addCol("������Ʒ��", allCols[7]);
                continue;
            }

            if ("ipv".equals(allCols[0])) {
                summay.addCol("IPV", allCols[1]);
                summay.addCol("IPV-UV", allCols[2]);
                continue;
            }

            if ("trade".equals(allCols[0])) {
                summay.addCol("֧�������", StringUtils.isNumeric(allCols[1]) ? allCols[1] : "0");
                summay.addCol("����UV", allCols[3]);
                summay.addCol("������", allCols[5]);
                continue;
            }

            if ("memeber_cart".equals(allCols[0])) {
                summay.addCol("���빺�ﳵ�ܴ���(��Ա)", allCols[1]);
                continue;
            }

            if ("guest_cart".equals(allCols[0])) {
                summay.addCol("���빺�ﳵ�ܴ���(�ÿ�)", allCols[1]);
                continue;
            }

            if ("error_item".equals(allCols[0])) {

                errorItem
                        .addCol(
                                String
                                        .format(
                                                "<a href=\"http://item.taobao.com/item.htm?id=%s\" title=\"%s\" target=\"_blank\" >%s</a>",
                                                allCols[2], allCols[3], allCols[2])).addCol(allCols[1]).addCol(
                                allCols[4]).addCol(allCols[5]).addCol(allCols[9]).addCol(Report.BREAK_VALUE);
                continue;
            }

            Table table = tableMap.get(allCols[0]);
            if (table != null) {
                if ("auction_status".equals(allCols[0])) {
                    table.addCol(auctionStatusMap.get(allCols[1]), allCols[2]);
                } else {
                    table.addCol(allCols[1], allCols[2]);
                }
            }
        }

        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
    }
}
