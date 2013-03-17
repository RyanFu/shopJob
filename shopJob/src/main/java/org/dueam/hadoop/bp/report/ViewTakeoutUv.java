package org.dueam.hadoop.bp.report;

import org.apache.commons.lang.StringUtils;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: zhanyi.wty
 * Date: 11-11-28
 * Time: ����1:25
 * To change this template use File | Settings | File Templates.
 */
public class ViewTakeoutUv {
    private static char CTRL_A = (char) 0x01;

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


        Report report = Report.newReport(name + "���н��ױ���");

        Table table = report.newViewTable("trade_takeout_rate", args[1] + "���н������ݱ���");
        table.addCol("ʡ��").addCol("����").addCol("ȫվ��ԱUV").addCol("ȫվ�ÿ�UV")
                .addCol("����LIST��ԱUV").addCol("����LIST�ÿ�UV").addCol("��ƷLIST��ԱUV").addCol("��ƷLIST�ÿ�UV")
                .addCol("���̻�ԱIPV_UV").addCol("���̷ÿ�IPV_UV").addCol("��Ʒ��ԱIPV_UV").addCol("��Ʒ�ÿ�IPV_UV")
                .addCol("����ҳ��ԱUV").addCol(Report.BREAK_VALUE);

        for (String line : Utils.readWithCharset(input, "utf-8")) {
            String[] _allCols = StringUtils.splitPreserveAllTokens(line, CTRL_A);
            table.addCol(_allCols[0]).addCol(_allCols[1]).addCol(_allCols[2]).addCol(_allCols[3])
                    .addCol(_allCols[4]).addCol(_allCols[5]).addCol(_allCols[6]).addCol(_allCols[7])
                    .addCol(_allCols[8]).addCol(_allCols[9]).addCol(_allCols[10]).addCol(_allCols[11])
                    .addCol(_allCols[12]).addCol(Report.BREAK_VALUE);

        }

        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
    }
}
