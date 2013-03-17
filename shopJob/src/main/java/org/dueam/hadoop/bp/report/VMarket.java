package org.dueam.hadoop.bp.report;

import org.apache.commons.lang.StringUtils;
import org.dueam.hadoop.common.util.Fmt;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 女装卖家统计
 */
public class VMarket {
    private static char CTRL_A = (char) 0x01;
    static Map<String, String> typeMap = Utils.asLinkedMap("1410", "甜美优雅", "1474", "街头潮人", "1538", "气质通勤", "1602", "胖MM装", "1666", "中老年装", "1858", "婚纱礼服", "1730","职业套装", "1794", "舞台服装");

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

        Report report = Report.newReport("风格馆 & 特色馆卖家相关线数据统计");
        Table main = report.newViewTable("total", "各个风格馆基本信息");
        main.addCol("馆分类").addCol("在售商品数").addCol("交易额")
                .addCol("购买UV").addCol("IPV").addCol("IPV-UV").addCol("购买转化率").addCol(Report.BREAK_VALUE);
        Map<String, Table> tableMap = new LinkedHashMap<String, Table>();
        Map<String, String[]> totalMap = new HashMap<String, String[]>();
        for (String key : typeMap.keySet()) {
            tableMap.put(key, report.newTable("table_" + key, typeMap.get(key) + "馆"));
            totalMap.put(key, new String[7]);
            totalMap.get(key)[0] = typeMap.get(key) + "馆";
        }

        for (String line : Utils.readWithCharset(input, "utf-8")) {
            String[] _allCols = StringUtils.splitPreserveAllTokens(line, CTRL_A);
            String type = _allCols[0];
            String tag = _allCols[1];
            Table table = tableMap.get(tag);
            if ("item".equals(type)) {
                //在线
                String namePrefix = "1".equals(_allCols[2]) ? "在售" : "仓库中";
                table.addCol(_allCols[2] + "_item_count", namePrefix + "商品数", _allCols[3]);
                table.addCol(_allCols[2] + "_item_count_sold", namePrefix + "商品数(有销量)", _allCols[4]);
                table.addCol(_allCols[2] + "_item_count_hb", namePrefix + "商品数(有画报)", _allCols[5]);
                if ("1".equals(_allCols[2])) {
                    totalMap.get(tag)[1] = _allCols[3];
                }
            } else if ("ipv".equals(type)) {
                table.addCol("IPV", _allCols[2]);
                totalMap.get(tag)[4] = _allCols[2];
                table.addCol("IPV-UV", _allCols[3]);
                totalMap.get(tag)[5] = _allCols[3];
                table.addCol("IPV-UV(MID)", _allCols[4]);
            } else if ("trade".equals(type)) {
                table.addCol("TRADE_TOTAL_FEE", "支付宝交易额", _allCols[2]);
                totalMap.get(tag)[2] = Fmt.moneyFmt(_allCols[2]);
                table.addCol("TRADE-ITEM-UV", "有交易的商品数", _allCols[3]);
                table.addCol("TRADE-BUYER-UV", "购买UV", _allCols[4]);
                totalMap.get(tag)[3] = _allCols[4];
                table.addCol("TRADE-SELLER-UV", "产生交易的卖家数", _allCols[5]);
            }

        }

        for (String[] cos : totalMap.values()) {
            cos[6] = Fmt.parent2(cos[3], cos[5]);
        }
        for (String[] cos : totalMap.values()) {
            for (String c : cos) {
                main.addCol(c);
            }
            main.addCol(Report.BREAK_VALUE);
        }


        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
    }

}
