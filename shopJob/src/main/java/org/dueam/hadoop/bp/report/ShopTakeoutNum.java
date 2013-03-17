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
public class ShopTakeoutNum {
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
        table.addCol("城市").addCol("店铺总数").addCol("在线店铺数").addCol("歇业店铺数").addCol("支付宝成交店铺数")
                .addCol("店铺动销率").addCol(Report.BREAK_VALUE);

        int shop_num = 0;
        int ol_shop_num = 0;
        int close_shop_num = 0;
        int ali_shop_num = 0;

        for (String line : Utils.readWithCharset(input, "utf-8")) {
            String[] _allCols = StringUtils.splitPreserveAllTokens(line, CTRL_A);
            table.addCol(_allCols[0]).addCol(_allCols[1]).addCol(_allCols[2]).addCol(_allCols[3])
                    .addCol(_allCols[4]).addCol(_allCols[5] + "%").addCol(Report.BREAK_VALUE);
            shop_num        = shop_num + Integer.parseInt(_allCols[1]);
            ol_shop_num     = ol_shop_num + Integer.parseInt(_allCols[2]);
            close_shop_num  = close_shop_num + Integer.parseInt(_allCols[3]);
            ali_shop_num    = ali_shop_num + Integer.parseInt(_allCols[4]);
        }
        table.addCol("合计").addCol(String.valueOf(shop_num)).addCol(String.valueOf(ol_shop_num))
                .addCol(String.valueOf(close_shop_num)).addCol(String.valueOf(ali_shop_num))
                .addCol("").addCol(Report.BREAK_VALUE);

        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
    }
}
