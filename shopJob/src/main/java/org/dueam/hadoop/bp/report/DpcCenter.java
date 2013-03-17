package org.dueam.hadoop.bp.report;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.dueam.hadoop.common.util.ItemUtils;
import org.dueam.hadoop.common.util.MapUtils;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.hadoop.services.Category;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DpcCenter {
    static Map<String, String> keyMap = Utils.asLinkedMap("all",
            "总量",
            "online",
            "线上商品",
            "line",
            "线下商品",
            "brand",
            "品牌授权商品",
            "jingxiao",
            "经销商品",
            "daixiao",
            "代销商品",
            "today_modified" ,
            "今天修改的记录数" ,
            "today_last_modified" ,
            "今天卖家修改过的记录数"
            );

    static String getKeyName(String key) {
        if (keyMap.containsKey(key)) {
            return keyMap.get(key);
        }
        return key;
    }



    /**
     * @param args
     * @throws java.io.IOException
     */
    public static void main(String[] args) throws IOException {
        Report report = Report.newReport("分销商品追踪中心");
        String input = args[0];
        if (!new File(input).exists()) {
            System.out.println("File Not Exist ! => " + input);
            return;
        }
        Map<String, List<String[]>> today = MapUtils.map(Utils.read(input, (String[])null));
        if (today == null) {
            System.out.println("No Data ! => " + input);
            return;
        }
        if (true && today.containsKey("dpc_total")) {
            Map<String, String[]> statusMap = MapUtils.toMap(today.get("dpc_total"));
            if (true) {
                Table table = report.newTable("total", "全网分销商品分布");
                for (String key : keyMap.keySet()) {
                    if (null != statusMap.get(key) && statusMap.get(key).length > 0) {
                        table.addCol(key, keyMap.get(key), statusMap.get(key)[0]);
                    }
                }
            }


        }

        if (true && today.containsKey("dpc_today")) {
            Map<String, String[]> statusMap = MapUtils.toMap(today.get("dpc_today"));
            if (true) {
                Table table = report.newTable("today", "今天发布的分销商品分布");
                for (String key : keyMap.keySet()) {
                    if (null != statusMap.get(key) && statusMap.get(key).length > 0) {
                        table.addCol(key, keyMap.get(key), statusMap.get(key)[0]);
                    }
                }
            }


        }

        if (true && today.containsKey("db_status")) {
            Table table = report.newGroupTable("status_all", "全网分销商品的各个状态分布");
            Map<String, String[]> statusMap = MapUtils.toMap(today.get("db_status"));
            for (String status : ItemUtils.allStatus) {
                String[] _vArray = statusMap.get(status);
                if (_vArray == null || _vArray.length < 1) {
                    table.addCol(status, ItemUtils.getItemStatusName(status), "0");
                } else {
                    table.addCol(status, ItemUtils.getItemStatusName(status), _vArray[0]);
                }
            }
        }

        if (true && today.containsKey("today_status")) {
            Table table = report.newGroupTable("status_today", "今天发布的经销商品各个状态分布");
            Map<String, String[]> statusMap = MapUtils.toMap(today.get("today_status"));
            for (String status : ItemUtils.allStatus) {
                String[] _vArray = statusMap.get(status);
                if (_vArray == null || _vArray.length < 1) {
                    table.addCol(status, ItemUtils.getItemStatusName(status), "0");
                } else {
                    table.addCol(status, ItemUtils.getItemStatusName(status), _vArray[0]);
                }
            }
        }

        if (true && today.containsKey("db_category")) {
            Map<String, Long> _countMap = new HashMap<String, Long>();
            for (String[] array : today.get("db_category")) {
                String catId = array[0];
                if (null != Category.getCategory(catId)) {
                    catId = Category.getCategory(catId).getRootId();
                }
                long value = NumberUtils.toLong(array[1]);
                Long _v = _countMap.get(catId);
                if (_v == null) {
                    _v = 0L;
                }
                _v = _v + value;
                _countMap.put(catId, _v);
            }
            Table table = report.newGroupTable("db_category", "全网分销商品各个类目分布");
            for (Map.Entry<String, Long> entry : _countMap.entrySet()) {
                table.addCol(entry.getKey(), Category.getCategoryName(entry.getKey()), String.valueOf(entry.getValue()));
            }
            table.sort(Table.SORT_VALUE);

        }

        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
    }


}
