package org.dueam.hadoop.bp.report;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.dueam.hadoop.common.util.Fmt;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

/**
 * 双12商品跟踪报表
 */
public class D12ItemReport {
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

        Report report = Report.newReport("双12商品跟踪报表");

        Table summay = report.newTable("summay", "基本详情");

        Map<String, Table> tableMap = new HashMap<String, Table>();

        tableMap.put("zk_type", report.newTable("zk_type", "折扣分布"));
        tableMap.put("zk_time", report.newTable("zk_time", "折扣时间分布"));
        tableMap.put("quantity_type", report.newTable("quantity_type", "库存分布"));
        tableMap.put("auction_status", report.newTable("auction_status", "状态分布"));
        tableMap.put("category", report.newTable("category", "一级类目分布"));

        Table errorItem = report.newViewTable("error_item", "问题商品信息(折扣小于2折,大于等于10折)");
        errorItem.addCol("商品id").addCol("一级类目").addCol("卖家").addCol("折扣(千分比)").addCol("吊牌价(分)").addCol(
                Report.BREAK_VALUE);

        for (String line : Utils.readWithCharset(input, "utf-8")) {
            String[] allCols = StringUtils.splitPreserveAllTokens(line, CTRL_A);

            if ("summay".equals(allCols[0])) {
                summay.addCol("商品数", allCols[1]);
                summay.addCol("总库存", allCols[2]);
                summay.addCol("平均库存", Fmt.parent4(allCols[2], allCols[1]));
                summay.addCol("在线商品数", allCols[3]);
                summay.addCol("一口价商品数", allCols[4]);
                summay.addCol("商城商品数", allCols[5]);
                summay.addCol("总收藏量", allCols[6]);
                summay.addCol("平均收藏量", Fmt.parent4(allCols[6], allCols[1]));
                summay.addCol("包邮商品数", allCols[7]);
                continue;
            }

            if ("ipv".equals(allCols[0])) {
                summay.addCol("IPV", allCols[1]);
                summay.addCol("IPV-UV", allCols[2]);
                continue;
            }

            if ("trade".equals(allCols[0])) {
                summay.addCol("支付宝金额", StringUtils.isNumeric(allCols[1]) ? allCols[1] : "0");
                summay.addCol("购买UV", allCols[3]);
                summay.addCol("订单数", allCols[5]);
                continue;
            }

            if ("memeber_cart".equals(allCols[0])) {
                summay.addCol("加入购物车总次数(会员)", allCols[1]);
                continue;
            }

            if ("guest_cart".equals(allCols[0])) {
                summay.addCol("加入购物车总次数(访客)", allCols[1]);
                continue;
            }

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

            Table table = tableMap.get(allCols[0]);
            if (table != null) {
                if ("auction_status".equals(allCols[0])) {
                    table.addCol(auctionStatusMap.get(allCols[1]), allCols[2]);
                } else {
                    table.addCol(allCols[1], allCols[2]);
                }
            }
        }

        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
    }
}
