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
 * Time: 下午1:25
 * To change this template use File | Settings | File Templates.
 */
public class ViewTakeoutKeyword {
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

        Table table = report.newViewTable("trade_takeout_rate", args[1] + "城市交易数据报表");
        table.addCol("关键词").addCol("会员PV").addCol("会员UV").addCol("店铺Detail会员PV").addCol("店铺Detail会员UV")
                .addCol("关键词引导成交UV").addCol(Report.BREAK_VALUE);

        for (String line : Utils.readWithCharset(input, "utf-8")) {
            String[] _allCols = StringUtils.splitPreserveAllTokens(line, CTRL_A);
            table.addCol(_allCols[1]).addCol(_allCols[2]).addCol(_allCols[3])
                    .addCol(_allCols[6]).addCol(_allCols[7])
                    .addCol(_allCols[10]).addCol(Report.BREAK_VALUE);

        }

        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
    }
}
