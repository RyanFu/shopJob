package org.dueam.hadoop.bp.report;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.dueam.hadoop.common.tables.Sku;
import org.dueam.hadoop.common.util.Fmt;
import org.dueam.hadoop.common.util.MapUtils;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.dueam.hadoop.common.tables.Cart.status;

/**
 * User: windonly
 * Date: 11-1-6 下午5:24
 */
public class SkuCenter {
    public static void main(String[] args) throws IOException {
        org.dueam.report.common.Report report = org.dueam.report.common.Report.newReport("SKU相关统计报表");
        String input = args[0];
        if (!new File(input).exists()) {
            return;
        }
        Map<String, List<String[]>> today = MapUtils.map(Utils.read(input, null));
        if (true && today.containsKey("sum")) {
            Map<String,String> map = MapUtils.toSimpleMap(today.get("sum"));
            Table table = report.newTable("sum", "基本指标", "纬度", "数量");
            table.addCol("sku","总记录数",map.get("sku"));
            table.addCol("sku_status_1","记录数（正常）",map.get("sku_status_1"));
            table.addCol("sku_status_-1","记录数（删除）",map.get("sku_status_-1"));
            table.addCol("item_sum","SKU总数（正常）",map.get("item"));
            table.addCol("item_count","商品数（正常）",map.get("item_count"));
            table.addCol("outer_sum","SKU总数（正常 - 有商家编码）",map.get("outer"));
            table.addCol("outer_count","商家编码数量",map.get("outer_count"));
            table.addCol("created","今天创建的记录数",map.get("created"));
            table.addCol("modified","今天修改的记录数",map.get("modified"));
        }

        if (true && today.containsKey("item")) {
            Table table = report.newGroupTable("item_area", "单个商品下SKU数量分布", "纬度", "数量");
            Map<String,String> map = MapUtils.toSimpleMap(today.get("item"));
            for (String key : Sku.areaKeys) {
                if(StringUtils.isNotEmpty(map.get(key))){
                    table.addCol(key,map.get(key));
                }
            }
        }


        if (true && today.containsKey("outer")) {
            Table table = report.newGroupTable("outer_area", "单个商家编码下SKU数量分布", "纬度", "数量");
            Map<String,String> map = MapUtils.toSimpleMap(today.get("outer"));
            for (String key : Sku.areaKeys) {
                if(StringUtils.isNotEmpty(map.get(key))){
                    table.addCol(key,map.get(key));
                }
            }
        }

        if (true && today.containsKey("seller")) {
            Table table = report.newGroupTable("seller_area", "单个卖家下面的商家编码数量分布", "纬度", "数量");
            Map<String,Long> map = new HashMap<String,Long>();
            for(String[] array : today.get("seller")){
                long count = NumberUtils.toLong(array[1]);
                String key = Sku.getArea(count);
                long sumCount = 0;
                if(map.containsKey(key)){
                    sumCount = map.get(key);
                }
                sumCount ++;
                map.put(key,sumCount);
            }

            for (String key : Sku.areaKeys) {
                if(map.get(key) !=null && map.get(key) > 0){
                    table.addCol(key,String.valueOf(map.get(key)));
                }
            }
        }

        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));

    }
}
