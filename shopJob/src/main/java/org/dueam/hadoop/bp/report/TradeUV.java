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
 * Date: 10-12-20 ����6:08
 */
public class TradeUV {
    public static void main(String[] args) throws Exception {
        String input = args[0];
        if (!new File(input).exists()) {
            return;
        }

        Map<String, String> today = MapUtils.toSimple(Utils.read(input, null));
        Report report = Report.newReport("����UV����");
        if (true) {
            Table table = report.newTable("trade_uv", "����UV");
            for (Map.Entry<String, String> entry : KEY_MAP.entrySet()) {
                if (today.containsKey(entry.getKey())) {
                    if (entry.getKey().endsWith("sum")) {
                        table.addCol(entry.getKey(), entry.getValue(), Utils.toYuan(today.get(entry.getKey())));
                    } else {
                        table.addCol(entry.getKey(), entry.getValue(), today.get(entry.getKey()));
                    }
                }
            }
            table.addCol("alipay_buyer_uv_parent", "֧���� - ���ﳵ����UVռ��(��)", Fmt.milli(today.get("cart_alipay_buyer_uv"), today.get("alipay_buyer_uv")));
            table.addCol("gmv_buyer_uv_parent", "GMV - ���ﳵ����UVռ��(��)", Fmt.milli(today.get("cart_gmv_buyer_uv"), today.get("gmv_buyer_uv")));
        }


        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
    }


    private static Map<String, String> KEY_MAP = MapUtils.asMap(new String[]{
            "cart_alipay_buyer_uv", "֧����(���ﳵ) - �����",
            "cart_alipay_seller_uv", "֧����(���ﳵ) - ������",
            "cart_alipay_trade_detail_pv", "֧����(���ﳵ) - �Ӷ�����",
            "cart_alipay_trade_main_pv", "֧����(���ﳵ) - ��������",
            "cart_alipay_trade_main_sum", "֧����(���ﳵ) - ���׶�",
            "alipay_buyer_uv", "֧���� - �����",
            "alipay_seller_uv", "֧���� - ������",
            "alipay_trade_detail_pv", "֧���� - �Ӷ�����",
            "alipay_trade_main_pv", "֧���� - ��������",
            "alipay_trade_main_sum", "֧���� - ���׶�",
            "cart_gmv_buyer_uv", "GMV(���ﳵ) - �����",
            "cart_gmv_seller_uv", "GMV(���ﳵ)- ������",
            "cart_gmv_trade_detail_pv", "GMV(���ﳵ) - �Ӷ�����",
            "cart_gmv_trade_main_pv", "GMV(���ﳵ) - ��������",
            "cart_gmv_trade_main_sum", "GMV(���ﳵ) - ���׶�",
            "gmv_buyer_uv", "GMV - �����",
            "gmv_seller_uv", "GMV - ������",
            "gmv_trade_detail_pv", "GMV - �Ӷ�����",
            "gmv_trade_main_pv", "GMV - ��������",
            "gmv_trade_main_sum", "GMV - ���׶�"

    });


}