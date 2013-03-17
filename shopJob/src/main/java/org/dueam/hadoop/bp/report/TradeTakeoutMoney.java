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
 * Time: 下午1:25
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


        Report report = Report.newReport(name + "城市交易报表");

        Table table = report.newViewTable("trade_takeout_money", args[1] + "城市交易数据报表");
        table.addCol("省份").addCol("城市").addCol("支付宝成交金额").addCol("GMV成交金额").addCol("支付宝使用率").addCol(Report.BREAK_VALUE);

        float ali_fee = 0;
        float gmv_fee = 0;

        for (String line : Utils.readWithCharset(input, "utf-8")) {
            String[] _allCols = StringUtils.splitPreserveAllTokens(line, CTRL_A);
            table.addCol(_allCols[0]).addCol(_allCols[1]).addCol(_allCols[3]).addCol(_allCols[2]).addCol(_allCols[4]).addCol(Report.BREAK_VALUE);
            ali_fee = ali_fee + Float.parseFloat(_allCols[3]);
            gmv_fee = gmv_fee + Float.parseFloat(_allCols[2]);
        }
        table.addCol("合计").addCol("").addCol(String.valueOf(ali_fee)).addCol(String.valueOf(gmv_fee)).addCol("").addCol(Report.BREAK_VALUE);

        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
    }
}
