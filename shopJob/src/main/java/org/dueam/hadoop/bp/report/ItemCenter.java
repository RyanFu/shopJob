package org.dueam.hadoop.bp.report;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.dueam.hadoop.common.util.ItemUtils;
import org.dueam.hadoop.common.util.MapUtils;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.hadoop.services.Category;
import org.dueam.hadoop.utils.TaobaoPath;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ItemCenter {
    static Map<String, String> keyMap = Utils.asMap("total_today",
            "今天 - 发布商品总数",
            "today_ata",
            "今天 - 拍卖商品",
            "today_atb",
            "今天 - 一口价商品",
            "today_ate",
            "今天 - 团购商品",
            "today_ps1",
            "今天 - 橱窗推荐商品",
            "today_ps2",
            "今天 - 橱窗推荐商品(小二)",
            "today_ss5",
            "今天 - 全新商品",
            "today_ss6",
            "今天 - 二手商品",
            "today_ss8",
            "今天 - 闲置商品",
            "total_all",
            "全网 - 商品总数",
            "today_modified",
            "今天 - 修改过的商品",
            "today_last_modified",
            "今天 - 卖家编辑过的商品",
            "db_ata",
            "全网 - 拍卖商品",
            "db_atb",
            "全网 - 一口价商品",
            "db_ate",
            "全网 - 团购商品",
            "db_ps1",
            "全网 - 橱窗推荐商品",
            "db_ps2",
            "全网 - 橱窗推荐商品(小二)",
            "db_ss5",
            "全网 - 全新商品",
            "db_ss6",
            "全网 - 二手商品",
            "db_ss8",
            "全网 - 闲置商品");

    static String getKeyName(String key) {
        if (keyMap.containsKey(key)) {
            return keyMap.get(key);
        }
        return key;
    }

    static Map<String, String> totalMap = Utils.asLinkedMap(
            "total_all",
            "全网 - 商品总数",
            "total_b",
            "全网 - 商品总数（商城）",
            "total_c",
            "全网 - 商品总数（C2C）",
            "total_hitao",
            "全网 - 商品总数（嗨淘）",
            "total_today",
            "今天 - 发布商品总数",
            "today_b",
            "今天 - 发布商品总数（商城）",
            "today_c",
            "今天 - 发布商品总数（C2C）",
            "today_hitao",
            "今天 - 发布商品总数（嗨淘）",
            "today_last_modified",
            "今天 - 卖家编辑过的商品",
            "today_modified",
            "今天 - 修改过的商品",
            "db_ps1",
            "全网 - 橱窗推荐商品",
            "db_ps2",
            "全网 - 橱窗推荐商品(小二)",
            "today_ps1",
            "今天 - 橱窗推荐商品",
            "today_ps2",
            "今天 - 橱窗推荐商品(小二)");

    //商品类型分布
    static Map<String, String> auctionTypeMap = Utils.asLinkedMap("db_atb",
            "全网 - 一口价商品", "db_ata",
            "全网 - 拍卖商品",
            "db_ate",
            "全网 - 团购商品",
            "today_atb",
            "今天 - 一口价商品",
            "today_ata",
            "今天 - 拍卖商品",

            "today_ate",
            "今天 - 团购商品");
    static Map<String, String> ssMap = Utils.asLinkedMap("db_ss5",
            "全网 - 全新商品",
            "db_ss6",
            "全网 - 二手商品",
            "db_ss8",
            "全网 - 闲置商品",

            "today_ss5",
            "今天 - 全新商品",
            "today_ss6",
            "今天 - 二手商品",
            "today_ss8",
            "今天 - 闲置商品");
    //橱窗推荐商品分布

    /**
     * @param args
     * @throws java.io.IOException
     */
    public static void main(String[] args) throws IOException {
        Report report = Report.newReport("商品中心");
        String input = args[0];
        if (!new File(input).exists()) {
            System.out.println("File Not Exist ! => " + input);
            return;
        }
        Map<String, List<String[]>> today = MapUtils.map(Utils.read(input, (String[]) null));
        if (today == null) {
            System.out.println("No Data ! => " + input);
            return;
        }
        if (true && today.containsKey("total")) {
            Map<String, String[]> statusMap = MapUtils.toMap(today.get("total"));
            if (true) {
                Table table = report.newTable("total", "全网商品总览");
                for (String key : totalMap.keySet()) {
                    if (null != statusMap.get(key) && statusMap.get(key).length > 0) {
                        table.addCol(key, totalMap.get(key), statusMap.get(key)[0]);
                    }
                }
            }

            if (true) {
                Table table = report.newTable("total_at", "商品类型分布");
                for (String key : auctionTypeMap.keySet()) {
                    if (null != statusMap.get(key) && statusMap.get(key).length > 0) {
                        table.addCol(key, auctionTypeMap.get(key), statusMap.get(key)[0]);
                    }
                }
            }
            if (true) {
                Table table = report.newTable("total_ss", "商品新旧属性分布");
                for (String key : ssMap.keySet()) {
                    if (null != statusMap.get(key) && statusMap.get(key).length > 0) {
                        table.addCol(key, ssMap.get(key), statusMap.get(key)[0]);
                    }
                }
            }

        }
        if (true && today.containsKey("db_status")) {
            Table table = report.newGroupTable("status_all", "全网商品的各个状态分布");
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
            Table table = report.newGroupTable("status_today", "今天发布的商品各个状态分布");
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

        Map<String,Map<String,Long>> catMap = new HashMap<String, Map<String,Long>>();
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
            Table table = report.newGroupTable("db_category", "全网商品各个类目分布");
            for (Map.Entry<String, Long> entry : _countMap.entrySet()) {
                table.addCol(entry.getKey(), Category.getCategoryName(entry.getKey()), String.valueOf(entry.getValue()));
            }
            table.sort(Table.SORT_VALUE);

            catMap.put("db_category",_countMap);

        }

        for (String status : ItemUtils.allStatus) {
            String statusKey = "db_category_" + status;
            if (true && today.containsKey(statusKey)) {
                Map<String, Long> _countMap = new HashMap<String, Long>();
                for (String[] array : today.get(statusKey)) {
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
                Table table = report.newGroupTable(statusKey, "全网" + ItemUtils.getItemStatusName(status) + "商品各个类目分布");
                for (Map.Entry<String, Long> entry : _countMap.entrySet()) {
                    table.addCol(entry.getKey(), Category.getCategoryName(entry.getKey()), String.valueOf(entry.getValue()));
                }
                table.sort(Table.SORT_VALUE);
                catMap.put(statusKey,_countMap);
            }
        }

        if(true && catMap.containsKey("db_category")){
            Table catTable = report.newViewTable("cat_view_table","商品状态分布一览") ;
            //catTable.addCol(Report.newValue(null,"类目ID"));
            catTable.addCol(Report.newValue(null,"一级类目"));
            catTable.addCol(Report.newValue(null,"总量"));
            for (String status : ItemUtils.allStatus) {
                catTable.addCol(Report.newValue(null,ItemUtils.getItemStatusName(status)));
            }
            catTable.addCol(Report.newBreakValue());
            Map<String, Long> _countMap = catMap.get("db_category")  ;
            for (Map.Entry<String, Long> entry : _countMap.entrySet()) {
                //catTable.addCol(Report.newValue(null,entry.getKey()));
                if(entry.getValue() < 50)continue;
                catTable.addCol(Report.newValue(null,Category.getCategoryName(entry.getKey())));
                catTable.addCol(Report.newValue(null,String.valueOf(entry.getValue())));
                for (String status : ItemUtils.allStatus) {
                    String statusKey = "db_category_" + status;
                    Map<String,Long> _statusMap = catMap.get(statusKey) ;
                    Long _v = _statusMap.get(entry.getKey());
                    String v = (_v == null ? "-":String.valueOf(_v) );
                    catTable.addCol(Report.newValue(null,v));
                }
                catTable.addCol(Report.newBreakValue());
            }
        }

        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
    }

    static String sum(Map<String, String> statusMap, String key) {
        long sum = 0;
        for (String status : ItemUtils.allStatus) {
            String newKey = key + "^" + status;
            sum += NumberUtils.toLong(statusMap.get(newKey) + "", 0);

        }
        if (sum <= 0) return "0";
        return String.valueOf(sum);
    }

    @SuppressWarnings("unchecked")
    static Map<String, String> getMap(String date) {
        try {
            Map<String, String> statusMap = new HashMap<String, String>();
            for (String line : (List<String>) FileUtils.readLines(new File(date), "GBK")) {
                String[] _array = StringUtils.split(line, "\t");
                String gmtDate = _array[0];
                String status = _array[1];
                String count = _array[2];
                statusMap.put(gmtDate + "^" + status, count);
            }
            return statusMap;
        } catch (Exception e) {
        }
        return null;
    }
}
