package org.dueam.hadoop.bp.report;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.dueam.hadoop.common.tables.TcRefundTrade;
import org.dueam.hadoop.common.util.Fmt;
import org.dueam.hadoop.common.util.MapUtils;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.hadoop.services.Category;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * User: windonly
 * Date: 10-12-20 下午6:08
 */
public class RefundCenter {
    public static void main(String[] args) throws Exception {
        String input = args[0];
        if (!new File(input).exists()) {
            return;
        }

        Map<String, List<String[]>> today = MapUtils.map(Utils.read(input, null));
        Report report = Report.newReport("交易退款报表");
        if (true && today.containsKey("total")) {
            Map<String, String> statusMap = MapUtils.toSimpleMap(today.get("total"));
            Table table = report.newTable("total", "退款表基本信息一览");
            for (Map.Entry<String, String> entry : TOTAL_TABLE.entrySet()) {
                if (statusMap.containsKey(entry.getKey())) {
                    String value = statusMap.get(entry.getKey());
                    if (StringUtils.endsWith(entry.getKey(), "fee")) {
                        value = Utils.toYuan(value);
                    }
                    table.addCol(entry.getKey(), entry.getValue(), value);
                }
            }

        }

        if (true && today.containsKey("cast_time")) {
            Map<String, String> statusMap = MapUtils.toSimpleMap(today.get("cast_time"));
            Table table = report.newGroupTable("cast_time", "今天完成退款的消耗时间分布");
            for (String key : TcRefundTrade.refundCastTimeArea.areaKeys) {
                if (statusMap.containsKey(key)) {
                    table.addCol(key, statusMap.get(key));
                }
            }

        }

        if (true && today.containsKey("cast_time_goods")) {
            Map<String, String> statusMap = MapUtils.toSimpleMap(today.get("cast_time_goods"));
            Table table = report.newGroupTable("cast_time_goods", "今天完成退款的消耗时间分布 - 需退货");
            for (String key : TcRefundTrade.refundCastTimeArea.areaKeys) {
                if (statusMap.containsKey(key)) {
                    table.addCol(key, statusMap.get(key));
                }
            }

        }

        if (true && today.containsKey("cast_time_no_goods")) {
            Map<String, String> statusMap = MapUtils.toSimpleMap(today.get("cast_time_no_goods"));
            Table table = report.newGroupTable("cast_time_no_goods", "今天完成退款的消耗时间分布 - 无退货");
            for (String key : TcRefundTrade.refundCastTimeArea.areaKeys) {
                if (statusMap.containsKey(key)) {
                    table.addCol(key, statusMap.get(key));
                }
            }

        }

        for (Map.Entry<String, String> group : GROUP_TABLE.entrySet()) {
            if (true && today.containsKey(group.getKey())) {
                Map<String, String> statusMap = MapUtils.toSimpleMap(today.get(group.getKey()));
                Table table = report.newGroupTable(group.getKey(), group.getValue());
                if (group.getKey().endsWith("refund_status")) {
                    for (Map.Entry<String, String> entry : REFUND_STATUS_TABLE.entrySet()) {
                        if (statusMap.containsKey(entry.getKey())) {
                            table.addCol(entry.getKey(), entry.getValue(), statusMap.get(entry.getKey()));
                        }
                    }
                }else if (group.getKey().endsWith("cs_status")) {
                    for (Map.Entry<String, String> entry : CS_STATUS_TABLE.entrySet()) {
                        if (statusMap.containsKey(entry.getKey())) {
                            table.addCol(entry.getKey(), entry.getValue(), statusMap.get(entry.getKey()));
                        }
                    }
                }
            }
        }


        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
    }


    private static Map<String, String> CS_STATUS_TABLE = MapUtils.asMap(new String[]{
            "1", "不需客服介入",
            "2", "需要客服介入",
            "3", "客服已经介入处理中",
            "4", "客服初审完成 ",
            "5", "客服主管复审失败",
            "6", "客服处理完成"
    });

    private static Map<String, String> TOTAL_TABLE = MapUtils.asMap(new String[]{
            "today_created", "创建的退款数 - 今天创建",
            "today_return_fee", "需要退款的金额 - 今天创建", // 今天
            "today_total_fee", "需要退款订单总金额 - 今天创建",
            "today_modified", "今天修改的记录数 ",
            "all", "全部记录数",
            "cast_time_sum", "今天完成的退款消耗的时间（秒）",
            "cast_time_num", "今天完成的退款的笔数"   ,
            "cast_time_goods_sum", "今天完成的退款消耗的时间（秒） - 需退货",
            "cast_time_goods_num", "今天完成的退款的笔数 - 需退货" ,
            "cast_time_no_goods_sum", "今天完成的退款消耗的时间（秒） - 无退货",
            "cast_time_no_goods_num", "今天完成的退款的笔数 - 无退货"
    });

    private static Map<String, String> REFUND_STATUS_TABLE = MapUtils.asMap(new String[]{
            "1", "退款协议等待卖家确认",
            "2", "退款协议已经达成，等待买家退货",
            "3", "买家已退货，等待卖家确认收货",
            "4", "退款关闭",
            "5", "退款成功",
            "6", "卖家不同意协议，等待买家修改"
    });

    private static Map<String, String> GROUP_TABLE = MapUtils.asMap(new String[]{
            "group_last_week_refund_status", "最近7天申请的退款状态统计",
            "group_last_week_cs_status", "最近7天申请的退款客服介入状态统计",
            "group_refund_status", "全表退款状态统计",
            "group_cs_status", "全表客服介入状态统计"
    });


}