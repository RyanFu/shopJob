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
public class ShopTakeoutAuction {
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
        table.addCol("城市").addCol("在线商品总数").addCol("新增商品总数").addCol("支付宝成交商品数")
                .addCol("商品动销率").addCol(Report.BREAK_VALUE);

        int ol_auc_num  = 0;
        int new_auc_num = 0;
        int ali_auc_num = 0;

        for (String line : Utils.readWithCharset(input, "utf-8")) {
            String[] _allCols = StringUtils.splitPreserveAllTokens(line, CTRL_A);
            table.addCol(_allCols[0]).addCol(_allCols[1]).addCol(_allCols[2]).addCol(_allCols[3])
                    .addCol(_allCols[4] + "%").addCol(Report.BREAK_VALUE);
            ol_auc_num   = ol_auc_num  + Integer.parseInt(_allCols[1]);
            new_auc_num  = new_auc_num + Integer.parseInt(_allCols[2]);
            ali_auc_num  = ali_auc_num + Integer.parseInt(_allCols[3]);
        }
        table.addCol("合计").addCol(String.valueOf(ol_auc_num)).addCol(String.valueOf(new_auc_num))
                .addCol(String.valueOf(ali_auc_num)).addCol("").addCol(Report.BREAK_VALUE);

        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
    }
}
