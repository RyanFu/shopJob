package org.dueam.hadoop.bp.report;

import org.dueam.hadoop.common.tables.BmwShops;
import org.dueam.hadoop.common.util.MapUtils;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class ShopCenter {

    static Map<String, String> totalMap = Utils.asLinkedMap(
            "all",
            "全网店铺数",
            "today",
            "今天新开店铺数");

    //商品类型分布
    static Map<String, String> approveStatusMap = Utils.asLinkedMap(
            "0", "正常",
            "2", "预释放",
            "-1", "关闭",
            "-2", "释放",
            "-3", "官方网店初始化第一步（设置店名）",
            "-4", "官方网店初始化第二步（设置域名）",
            "-5", "官方网店服务过期",
            "-9", "cc");
    static Map<String, String> psMap = Utils.asLinkedMap(
            "1", "特色店铺"
            , "2", "大品牌店铺"
            , "4", "实力店铺"
            , "8", "CPS全店推广店铺（淘客）"
            , "16", "机票商家 "
            , "32", "酒店商家"
            , "64", "保险商家"
            , "128", "旅游商家"
            , "1024", "店铺搜屏蔽店铺"
            , "2048", "阿里商城店铺");
    static Map<String, String> shopTypeMap = Utils.asLinkedMap(
            "4", "普通店铺"
            , "1", "旺铺扶植版"
            , "3", "旺铺标准版"
            , "7", "旺铺拓展版"
            , "2", "旺铺商城版"
            , "6", "旺铺机票版"
            , "5", "旺铺外店版"
            , "8", "开放搜索店铺"
            , "9", "无名良品");

    /**
     * @param args
     * @throws java.io.IOException
     */
    public static void main(String[] args) throws IOException {
        Report report = Report.newReport("店铺中心");
        String input = args[0];
        if (!new File(input).exists()) {
            System.out.println("File Not Exist ! => " + input);
            return;
        }
        Map<String, List<String[]>> today = MapUtils.map(Utils.read(input, null));
        if (today == null) {
            System.out.println("No Data ! => " + input);
            return;
        }
        if (true && today.containsKey("total")) {
            Map<String, String[]> statusMap = MapUtils.toMap(today.get("total"));
            Table table = report.newTable("total", "店铺信息总览");
            for (String key : totalMap.keySet()) {
                if (null != statusMap.get(key) && statusMap.get(key).length > 0) {
                    table.addCol(key, totalMap.get(key), statusMap.get(key)[0]);
                }
            }

        }

        if (true && today.containsKey("product_count")) {
            Table table = report.newGroupTable("product_count", "店铺商品数分布");
            Map<String, String[]> statusMap = MapUtils.toMap(today.get("product_count"));

            for (String key : BmwShops.areaKeys) {
                if (null != statusMap.get(key) && statusMap.get(key).length > 0) {
                    table.addCol(key, key, statusMap.get(key)[0]);
                }
            }
        }
        if (true && today.containsKey("approve_status")) {
            Table table = report.newGroupTable("approve_status", "店铺状态分布");
            Map<String, String[]> statusMap = MapUtils.toMap(today.get("approve_status"));

            for (String key : approveStatusMap.keySet()) {
                if (null != statusMap.get(key) && statusMap.get(key).length > 0) {
                    table.addCol(key, approveStatusMap.get(key), statusMap.get(key)[0]);
                }
            }
        }

        if (true && today.containsKey("site")) {
            Table table = report.newGroupTable("site", "店铺类型分布 - 全网");
            Map<String, String[]> statusMap = MapUtils.toMap(today.get("site"));

            for (String key : shopTypeMap.keySet()) {
                if (null != statusMap.get(key) && statusMap.get(key).length > 0) {
                    table.addCol(key, shopTypeMap.get(key), statusMap.get(key)[0]);
                }
            }
        }

        if (true && today.containsKey("today_site")) {
            Table table = report.newGroupTable("today_site", "店铺类型分布 - 今天新开");
            Map<String, String[]> statusMap = MapUtils.toMap(today.get("today_site"));

            for (String key : shopTypeMap.keySet()) {
                if (null != statusMap.get(key) && statusMap.get(key).length > 0) {
                    table.addCol(key, shopTypeMap.get(key), statusMap.get(key)[0]);
                }
            }
        }

        if (true && today.containsKey("promoted_status")) {
            Table table = report.newGroupTable("promoted_status", "店铺标识分布");
            Map<String, String[]> statusMap = MapUtils.toMap(today.get("promoted_status"));

            for (String key : psMap.keySet()) {
                if (null != statusMap.get(key) && statusMap.get(key).length > 0) {
                    table.addCol(key, psMap.get(key), statusMap.get(key)[0]);
                }
            }
        }


        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
    }
}
