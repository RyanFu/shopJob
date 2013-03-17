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
public class TradeTakeoutRate {
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
        table.addCol("ʡ��").addCol("����").addCol("֧�����ɽ�����").addCol("֧�����ɽ����")
                .addCol("�͵���").addCol("���������û���").addCol("�����û�����ת����")
                .addCol("���û�����ת����").addCol(Report.BREAK_VALUE);

        int ali_num = 0;
        float ali_fee = 0;
        int new_buy_num = 0;

        for (String line : Utils.readWithCharset(input, "utf-8")) {
            String[] _allCols = StringUtils.splitPreserveAllTokens(line, CTRL_A);
            table.addCol(_allCols[0]).addCol(_allCols[1]).addCol(_allCols[2]).addCol(_allCols[3])
                    .addCol(_allCols[4]).addCol(_allCols[5]).addCol(_allCols[6] + "%")
                    .addCol(_allCols[7] + "%").addCol(Report.BREAK_VALUE);
            ali_num = ali_num + Integer.valueOf(_allCols[2]);
            ali_fee = ali_fee + Float.parseFloat(_allCols[3]);
            new_buy_num = new_buy_num + Integer.valueOf(_allCols[5]);
        }

         table.addCol("�ϼ�").addCol("").addCol(String.valueOf(ali_num)).addCol(String.valueOf(ali_fee))
                 .addCol("").addCol(String.valueOf(new_buy_num)).addCol("")
                 .addCol("").addCol(Report.BREAK_VALUE);

        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
    }
}
