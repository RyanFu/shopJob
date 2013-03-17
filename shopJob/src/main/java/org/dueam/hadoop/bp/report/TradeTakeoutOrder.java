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
public class TradeTakeoutOrder {
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


        Report report = Report.newReport(name + "城市订单报表");

        Table table = report.newViewTable("trade_takeout_money", args[1] + "城市订单数据报表");
        table.addCol("城市").addCol("支付宝成交订单数").addCol("支付宝成交笔数").addCol("支付宝成交金额").addCol("笔单价")
                .addCol("支付宝成交件数").addCol("件单价").addCol(Report.BREAK_VALUE);

        int main_order_cnt = 0;
        int sub_order_cnt  = 0;
        float ali_fee = 0;
        int auc_num = 0;


        for (String line : Utils.readWithCharset(input, "utf-8")) {
            String[] _allCols = StringUtils.splitPreserveAllTokens(line, CTRL_A);
            table.addCol(_allCols[1]).addCol(_allCols[2]).addCol(_allCols[3]).addCol(_allCols[4]).addCol(_allCols[5])
                    .addCol(_allCols[6]).addCol(_allCols[7]).addCol(Report.BREAK_VALUE);

            main_order_cnt = main_order_cnt + Integer.valueOf(_allCols[2]);
            sub_order_cnt  = sub_order_cnt  + Integer.valueOf(_allCols[3]);
            ali_fee = ali_fee + Float.parseFloat(_allCols[4]);
            auc_num = auc_num + Integer.valueOf(_allCols[6]);

        }

        table.addCol("合计").addCol(String.valueOf(main_order_cnt)).addCol(String.valueOf(sub_order_cnt)).addCol(String.valueOf(ali_fee)).addCol("")
                    .addCol(String.valueOf(auc_num)).addCol("").addCol(Report.BREAK_VALUE);

        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
    }
}
