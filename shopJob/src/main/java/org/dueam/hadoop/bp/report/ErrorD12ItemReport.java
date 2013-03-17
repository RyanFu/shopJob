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
 * ˫12��Ʒ���ٱ���
 */
public class ErrorD12ItemReport {
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

        Report report = Report.newReport("˫12���������Ʒ����(һСʱ)");

        Table errorItem = report.newViewTable("error_item", "������Ʒ��Ϣ(�ۿ�С��1��,���ڵ���10��)");
        errorItem.addCol("��Ʒid").addCol("Ҷ����Ŀ").addCol("����").addCol("�ۿ�(ǧ�ֱ�)").addCol("���Ƽ�(��)").addCol(
                Report.BREAK_VALUE);

        for (String line : Utils.readWithCharset(input, "utf-8")) {
            String[] allCols = StringUtils.splitPreserveAllTokens(line, CTRL_A);

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

        }

        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
    }
}
