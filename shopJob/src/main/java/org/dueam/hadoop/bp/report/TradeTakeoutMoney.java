package org.dueam.hadoop.bp.report;

import org.apache.commons.lang.StringUtils;
import org.dueam.hadoop.common.util.MapUtils;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: zhanyi.wty
 * Date: 11-11-28
 * Time: ����1:25
 * To change this template use File | Settings | File Templates.
 */
public class TradeTakeoutMoney {
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

        Table table = report.newViewTable("trade_takeout_money", args[1] + "���н������ݱ���");
        table.addCol("ʡ��").addCol("����").addCol("֧�����ɽ����").addCol("GMV�ɽ����").addCol("֧����ʹ����").addCol(Report.BREAK_VALUE);

        float ali_fee = 0;
        float gmv_fee = 0;

        for (String line : Utils.readWithCharset(input, "utf-8")) {
            String[] _allCols = StringUtils.splitPreserveAllTokens(line, CTRL_A);
            table.addCol(_allCols[0]).addCol(_allCols[1]).addCol(_allCols[3]).addCol(_allCols[2]).addCol(_allCols[4]).addCol(Report.BREAK_VALUE);
            ali_fee = ali_fee + Float.parseFloat(_allCols[3]);
            gmv_fee = gmv_fee + Float.parseFloat(_allCols[2]);
        }
        table.addCol("�ϼ�").addCol("").addCol(String.valueOf(ali_fee)).addCol(String.valueOf(gmv_fee)).addCol("").addCol(Report.BREAK_VALUE);

        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
    }
}
