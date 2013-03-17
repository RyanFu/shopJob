package org.dueam.hadoop.bp.report;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.dueam.hadoop.common.CounterMap;
import org.dueam.hadoop.common.util.Fmt;
import org.dueam.hadoop.common.util.MapUtils;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.hadoop.services.Category;
import org.dueam.hadoop.services.ShopDomain;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.commons.lang.math.NumberUtils.toDouble;
import static org.apache.commons.lang.math.NumberUtils.toLong;
import static org.dueam.hadoop.common.Functions.newHashMap;
import static org.dueam.hadoop.common.Functions.str;

/**
 * 各个类目IPV，交易额统计
 */
public class TotalCat {
    private static char CTRL_A = (char) 0x01;

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

        String mainName = args[1];

        Report report = Report.newReport(mainName + "市场相关业务数据");

        Map<String, List<String[]>> todayMap = MapUtils.map(Utils.read(args[0]), CTRL_A);
        Map<String, List<String[]>> ipvMap = MapUtils.map(todayMap.get("ipv"));
        Map<String, List<String[]>> tradeMap = MapUtils.map(todayMap.get("trade"));
        Map<String, String> orderCountMap = newHashMap();   //浏览转化率 IPV/支付宝购买UV
        Map<String, String> buyerUvMap = newHashMap();
        if (todayMap.containsKey("trade")) {
            Table tradeTable = report.newViewTable("total_trade", mainName + "交易相关信息一览表");
            tradeTable.addCol("类目").addCol("交易额（元）").addCol("订单数").addCol("商品UV").addCol("购买UV").addCol("客单价").addCol(Report.BREAK_VALUE);
            Collections.sort(todayMap.get("trade"), new Comparator<String[]>() {
                public int compare(String[] o1, String[] o2) {
                    return NumberUtils.toLong(o1[3]) >= NumberUtils.toLong(o2[3]) ? -1 : 1;
                }
            });
            for (String[] _cols : todayMap.get("trade")) {
                String catId = "leafCat".equals(_cols[0]) ? _cols[2] : _cols[1];
                if (catId == null) catId = "NULL";
                orderCountMap.put(catId, _cols[8]);
                buyerUvMap.put(catId,_cols[6]) ;
                tradeTable.addCol(Category.getCategoryName(catId)).addCol(Fmt.moneyFmt(_cols[3]) + "(" + _cols[3] + ")").addCol(_cols[8]).addCol(_cols[5]).addCol(_cols[6]).addCol(Fmt.div(_cols[3], _cols[6])).addCol(Report.BREAK_VALUE);
            }
        }

        if (todayMap.containsKey("ipv")) {
            Table tradeTable = report.newViewTable("total_ipv", mainName + "浏览相关信息一览表");
            tradeTable.addCol("类目").addCol("IPV").addCol("浏览UV").addCol("会员UV").addCol("IPV/浏览UV").addCol("浏览转化率（%）").addCol("成交转化率（%）").addCol(Report.BREAK_VALUE);
            Collections.sort(todayMap.get("ipv"), new Comparator<String[]>() {
                public int compare(String[] o1, String[] o2) {
                    return NumberUtils.toLong(o1[3]) >= NumberUtils.toLong(o2[3]) ? -1 : 1;
                }
            });
            for (String[] _cols : todayMap.get("ipv")) {
                String catId = "leafCat".equals(_cols[0]) ? _cols[2] : _cols[1];
                if (catId == null) catId = "NULL";
                tradeTable.addCol(Category.getCategoryName(catId)).addCol(Fmt.moneyFmt(_cols[3])).addCol(_cols[4]).addCol(_cols[5]).addCol(Fmt.div(_cols[3], _cols[4])).addCol(Fmt.parent2(orderCountMap.get(catId),_cols[3])).addCol(Fmt.parent2(buyerUvMap.get(catId),_cols[5])).addCol(Report.BREAK_VALUE);
            }
        }
        /**
         CREATE EXTERNAL TABLE trade_category (
         type string,
         root_cat string,
         cat_id string,
         total_fee double,
         discount_fee double,
         item_uv bigint COMMENT '每日交易商品UV',
         buyer_uv bigint ,
         seller_uv bigint COMMENT '浏览用户UV')
         COMMENT '类目交易表'
         PARTITIONED BY(pt STRING)
         ROW FORMAT DELIMITED
         FIELDS TERMINATED BY '\t'
         STORED AS TEXTFILE;
         */
        if (tradeMap.containsKey("leafCat")) {
            Table totalFeeTable = report.newTable("trade_total_fee", mainName + "各个子类目支付宝交易额统计");
            Table discountTable = report.newTable("trade_discount_fee", mainName + "各个子类目折扣金额统计");
            Table itemUvTable = report.newTable("trade_item_uv", mainName + "各个子类目交易商品数统计");
            Table buyerUvTable = report.newTable("trade_buyer_uv", mainName + "各个子类目购买UV统计");
            Table sellerUvTable = report.newTable("trade_seller_uv", mainName + "各个子类目卖家UV统计");
            for (String[] _cols : tradeMap.get("rootCat")) {
                String catId = _cols[0] == null ? "NULL" : _cols[0];
                totalFeeTable.addCol(catId, Category.getCategoryName(catId), _cols[2]);
                discountTable.addCol(catId, Category.getCategoryName(catId), _cols[3]);
                itemUvTable.addCol(catId, Category.getCategoryName(catId), _cols[4]);
                buyerUvTable.addCol(catId, Category.getCategoryName(catId), _cols[5]);
                sellerUvTable.addCol(catId, Category.getCategoryName(catId), _cols[6]);
            }
            for (String[] _cols : tradeMap.get("leafCat")) {
                String catId = _cols[1] == null ? "NULL" : _cols[1];
                totalFeeTable.addCol(catId, Category.getCategoryName(catId), _cols[2]);
                discountTable.addCol(catId, Category.getCategoryName(catId), _cols[3]);
                itemUvTable.addCol(catId, Category.getCategoryName(catId), _cols[4]);
                buyerUvTable.addCol(catId, Category.getCategoryName(catId), _cols[5]);
                sellerUvTable.addCol(catId, Category.getCategoryName(catId), _cols[6]);
            }
            totalFeeTable.sort(Table.SORT_VALUE);
            discountTable.sort(Table.SORT_VALUE);
            itemUvTable.sort(Table.SORT_VALUE);
            buyerUvTable.sort(Table.SORT_VALUE);
            sellerUvTable.sort(Table.SORT_VALUE);
        }
        /**
         * CREATE EXTERNAL TABLE ipv_category (
         type string,
         root_cat string,
         cat_id string,
         ipv bigint,
         muv bigint COMMENT '机器UV',
         uv bigint COMMENT '浏览用户UV')
         COMMENT '叶子类目与一级类目的IPV'
         PARTITIONED BY(pt STRING)
         ROW FORMAT DELIMITED
         FIELDS TERMINATED BY '\t'
         STORED AS TEXTFILE;
         */
        if (ipvMap.containsKey("leafCat")) {
            Table ipvTable = report.newTable("ipv_ipv", mainName + "各个子类目IPV统计");
            Table muvTable = report.newTable("ipv_muv", mainName + "各个子类目浏览UV");
            Table uvTable = report.newTable("ipv_uv", mainName + "各个子类目会员UV");
            for (String[] _cols : ipvMap.get("rootCat")) {
                String catId = _cols[0] == null ? "NULL" : _cols[0];
                ipvTable.addCol(catId, Category.getCategoryName(catId), _cols[2]);
                muvTable.addCol(catId, Category.getCategoryName(catId), _cols[3]);
                uvTable.addCol(catId, Category.getCategoryName(catId), _cols[4]);
            }
            for (String[] _cols : ipvMap.get("leafCat")) {
                String catId = _cols[1] == null ? "NULL" : _cols[1];
                ipvTable.addCol(catId, Category.getCategoryName(catId), _cols[2]);
                muvTable.addCol(catId, Category.getCategoryName(catId), _cols[3]);
                uvTable.addCol(catId, Category.getCategoryName(catId), _cols[4]);
            }
            ipvTable.sort(Table.SORT_VALUE);
            muvTable.sort(Table.SORT_VALUE);
            uvTable.sort(Table.SORT_VALUE);
        }

        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
    }


}
