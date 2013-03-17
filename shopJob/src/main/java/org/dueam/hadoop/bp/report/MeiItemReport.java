package org.dueam.hadoop.bp.report;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.dueam.hadoop.common.util.Fmt;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

/**
 * 美妆商品报表
 */
public class MeiItemReport {
    private static char CTRL_A = (char) 0x01;

    private static Map<String, String> catMap = new HashMap<String, String>();

    static {

        catMap.put("1801", "美容护肤/美体/精油");
        catMap.put("50010788", "彩妆/香水/美妆工具");
        catMap.put("50023282", "美发护发/假发");

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

        Report report = Report.newReport("美妆市场商品交易报表");

        Map<String, Table> summayMap = new HashMap<String, Table>();

        for (Entry<String, String> entry : catMap.entrySet()) {
            Table summay = report.newTable(entry.getKey() + "summay", entry.getValue() + "--基本详情");
            summayMap.put(entry.getKey(), summay);
        }

        for (String line : Utils.readWithCharset(input, "utf-8")) {
            String[] allCols = StringUtils.splitPreserveAllTokens(line, CTRL_A);

            if ("summay".equals(allCols[0])) {

                Table summay = summayMap.get(allCols[1]);
                if (summay == null) {
                    continue;
                }

                summay.addCol("商品数", allCols[2]);
                summay.addCol("在线商品数", allCols[3]);
                summay.addCol("新发商品数", allCols[4]);
                summay.addCol("包邮商品数", allCols[5]);
                continue;
            }

            if ("ipv".equals(allCols[0])) {

                Table summay = summayMap.get(allCols[1]);
                if (summay == null) {
                    continue;
                }

                summay.addCol("IPV", allCols[2]);
                summay.addCol("IPV-UV", allCols[3]);
                continue;
            }

            if ("trade".equals(allCols[0])) {

                Table summay = summayMap.get(allCols[1]);
                if (summay == null) {
                    continue;
                }

                summay.addCol("支付宝金额", StringUtils.isNumeric(allCols[2]) ? allCols[2] : "0");
                summay.addCol("发生购买的商品数", allCols[3]);
                summay.addCol("发生购买的买家数", allCols[4]);
                summay.addCol("发生购买的卖家数", allCols[5]);
                summay.addCol("订单数", allCols[6]);
                summay.addCol("客单价", Fmt.parent4(allCols[2], allCols[4]));
                continue;
            }

            if ("memeber_cart".equals(allCols[0])) {

                Table summay = summayMap.get(allCols[1]);
                if (summay == null) {
                    continue;
                }

                summay.addCol("加入购物车总次数(会员)", allCols[2]);
                continue;
            }

            if ("guest_cart".equals(allCols[0])) {

                Table summay = summayMap.get(allCols[1]);
                if (summay == null) {
                    continue;
                }
                summay.addCol("加入购物车总次数(访客)", allCols[2]);
                continue;
            }

        }

        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
    }
}
