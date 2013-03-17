package org.dueam.hadoop.bp.report;

import org.dueam.hadoop.common.util.Fmt;
import org.dueam.hadoop.common.util.MapUtils;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Map;

/**
 * User: windonly
 * Date: 10-12-20 下午6:08
 */
public class TradeUV {
    public static void main(String[] args) throws Exception {
        String input = args[0];
        if (!new File(input).exists()) {
            return;
        }

        Map<String, String> today = MapUtils.toSimple(Utils.read(input, null));
        Report report = Report.newReport("交易UV报表");
        if (true) {
            Table table = report.newTable("trade_uv", "交易UV");
            for (Map.Entry<String, String> entry : KEY_MAP.entrySet()) {
                if (today.containsKey(entry.getKey())) {
                    if (entry.getKey().endsWith("sum")) {
                        table.addCol(entry.getKey(), entry.getValue(), Utils.toYuan(today.get(entry.getKey())));
                    } else {
                        table.addCol(entry.getKey(), entry.getValue(), today.get(entry.getKey()));
                    }
                }
            }
            table.addCol("alipay_buyer_uv_parent", "支付宝 - 购物车购买UV占比(‰)", Fmt.milli(today.get("cart_alipay_buyer_uv"), today.get("alipay_buyer_uv")));
            table.addCol("gmv_buyer_uv_parent", "GMV - 购物车购买UV占比(‰)", Fmt.milli(today.get("cart_gmv_buyer_uv"), today.get("gmv_buyer_uv")));
        }


        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
    }


    private static Map<String, String> KEY_MAP = MapUtils.asMap(new String[]{
            "cart_alipay_buyer_uv", "支付宝(购物车) - 买家数",
            "cart_alipay_seller_uv", "支付宝(购物车) - 卖家数",
            "cart_alipay_trade_detail_pv", "支付宝(购物车) - 子订单数",
            "cart_alipay_trade_main_pv", "支付宝(购物车) - 父订单数",
            "cart_alipay_trade_main_sum", "支付宝(购物车) - 交易额",
            "alipay_buyer_uv", "支付宝 - 买家数",
            "alipay_seller_uv", "支付宝 - 卖家数",
            "alipay_trade_detail_pv", "支付宝 - 子订单数",
            "alipay_trade_main_pv", "支付宝 - 父订单数",
            "alipay_trade_main_sum", "支付宝 - 交易额",
            "cart_gmv_buyer_uv", "GMV(购物车) - 买家数",
            "cart_gmv_seller_uv", "GMV(购物车)- 卖家数",
            "cart_gmv_trade_detail_pv", "GMV(购物车) - 子订单数",
            "cart_gmv_trade_main_pv", "GMV(购物车) - 父订单数",
            "cart_gmv_trade_main_sum", "GMV(购物车) - 交易额",
            "gmv_buyer_uv", "GMV - 买家数",
            "gmv_seller_uv", "GMV - 卖家数",
            "gmv_trade_detail_pv", "GMV - 子订单数",
            "gmv_trade_main_pv", "GMV - 父订单数",
            "gmv_trade_main_sum", "GMV - 交易额"

    });


}