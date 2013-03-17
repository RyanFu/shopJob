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
 * 双12商品跟踪报表
 */
public class ErrorD12ItemReport {
    private static char CTRL_A = (char) 0x01;

    private static Map<String, String> auctionStatusMap = new HashMap<String, String>();

    static {

        auctionStatusMap.put("0", "在售");
        auctionStatusMap.put("1", "小二确认");
        auctionStatusMap.put("-1", "用户删除");
        auctionStatusMap.put("-2", "用户下架");
        auctionStatusMap.put("-3", "小二下架");
        auctionStatusMap.put("-4", "小二删除");
        auctionStatusMap.put("-5", "从未上架");
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

        Report report = Report.newReport("双12打标疑问商品跟踪(一小时)");

        Table errorItem = report.newViewTable("error_item", "问题商品信息(折扣小于1折,大于等于10折)");
        errorItem.addCol("商品id").addCol("叶子类目").addCol("卖家").addCol("折扣(千分比)").addCol("吊牌价(分)").addCol(
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
