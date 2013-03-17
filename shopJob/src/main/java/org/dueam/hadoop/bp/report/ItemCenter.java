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
            "���� - ������Ʒ����",
            "today_ata",
            "���� - ������Ʒ",
            "today_atb",
            "���� - һ�ڼ���Ʒ",
            "today_ate",
            "���� - �Ź���Ʒ",
            "today_ps1",
            "���� - �����Ƽ���Ʒ",
            "today_ps2",
            "���� - �����Ƽ���Ʒ(С��)",
            "today_ss5",
            "���� - ȫ����Ʒ",
            "today_ss6",
            "���� - ������Ʒ",
            "today_ss8",
            "���� - ������Ʒ",
            "total_all",
            "ȫ�� - ��Ʒ����",
            "today_modified",
            "���� - �޸Ĺ�����Ʒ",
            "today_last_modified",
            "���� - ���ұ༭������Ʒ",
            "db_ata",
            "ȫ�� - ������Ʒ",
            "db_atb",
            "ȫ�� - һ�ڼ���Ʒ",
            "db_ate",
            "ȫ�� - �Ź���Ʒ",
            "db_ps1",
            "ȫ�� - �����Ƽ���Ʒ",
            "db_ps2",
            "ȫ�� - �����Ƽ���Ʒ(С��)",
            "db_ss5",
            "ȫ�� - ȫ����Ʒ",
            "db_ss6",
            "ȫ�� - ������Ʒ",
            "db_ss8",
            "ȫ�� - ������Ʒ");

    static String getKeyName(String key) {
        if (keyMap.containsKey(key)) {
            return keyMap.get(key);
        }
        return key;
    }

    static Map<String, String> totalMap = Utils.asLinkedMap(
            "total_all",
            "ȫ�� - ��Ʒ����",
            "total_b",
            "ȫ�� - ��Ʒ�������̳ǣ�",
            "total_c",
            "ȫ�� - ��Ʒ������C2C��",
            "total_hitao",
            "ȫ�� - ��Ʒ���������ԣ�",
            "total_today",
            "���� - ������Ʒ����",
            "today_b",
            "���� - ������Ʒ�������̳ǣ�",
            "today_c",
            "���� - ������Ʒ������C2C��",
            "today_hitao",
            "���� - ������Ʒ���������ԣ�",
            "today_last_modified",
            "���� - ���ұ༭������Ʒ",
            "today_modified",
            "���� - �޸Ĺ�����Ʒ",
            "db_ps1",
            "ȫ�� - �����Ƽ���Ʒ",
            "db_ps2",
            "ȫ�� - �����Ƽ���Ʒ(С��)",
            "today_ps1",
            "���� - �����Ƽ���Ʒ",
            "today_ps2",
            "���� - �����Ƽ���Ʒ(С��)");

    //��Ʒ���ͷֲ�
    static Map<String, String> auctionTypeMap = Utils.asLinkedMap("db_atb",
            "ȫ�� - һ�ڼ���Ʒ", "db_ata",
            "ȫ�� - ������Ʒ",
            "db_ate",
            "ȫ�� - �Ź���Ʒ",
            "today_atb",
            "���� - һ�ڼ���Ʒ",
            "today_ata",
            "���� - ������Ʒ",

            "today_ate",
            "���� - �Ź���Ʒ");
    static Map<String, String> ssMap = Utils.asLinkedMap("db_ss5",
            "ȫ�� - ȫ����Ʒ",
            "db_ss6",
            "ȫ�� - ������Ʒ",
            "db_ss8",
            "ȫ�� - ������Ʒ",

            "today_ss5",
            "���� - ȫ����Ʒ",
            "today_ss6",
            "���� - ������Ʒ",
            "today_ss8",
            "���� - ������Ʒ");
    //�����Ƽ���Ʒ�ֲ�

    /**
     * @param args
     * @throws java.io.IOException
     */
    public static void main(String[] args) throws IOException {
        Report report = Report.newReport("��Ʒ����");
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
                Table table = report.newTable("total", "ȫ����Ʒ����");
                for (String key : totalMap.keySet()) {
                    if (null != statusMap.get(key) && statusMap.get(key).length > 0) {
                        table.addCol(key, totalMap.get(key), statusMap.get(key)[0]);
                    }
                }
            }

            if (true) {
                Table table = report.newTable("total_at", "��Ʒ���ͷֲ�");
                for (String key : auctionTypeMap.keySet()) {
                    if (null != statusMap.get(key) && statusMap.get(key).length > 0) {
                        table.addCol(key, auctionTypeMap.get(key), statusMap.get(key)[0]);
                    }
                }
            }
            if (true) {
                Table table = report.newTable("total_ss", "��Ʒ�¾����Էֲ�");
                for (String key : ssMap.keySet()) {
                    if (null != statusMap.get(key) && statusMap.get(key).length > 0) {
                        table.addCol(key, ssMap.get(key), statusMap.get(key)[0]);
                    }
                }
            }

        }
        if (true && today.containsKey("db_status")) {
            Table table = report.newGroupTable("status_all", "ȫ����Ʒ�ĸ���״̬�ֲ�");
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
            Table table = report.newGroupTable("status_today", "���췢������Ʒ����״̬�ֲ�");
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
            Table table = report.newGroupTable("db_category", "ȫ����Ʒ������Ŀ�ֲ�");
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
                Table table = report.newGroupTable(statusKey, "ȫ��" + ItemUtils.getItemStatusName(status) + "��Ʒ������Ŀ�ֲ�");
                for (Map.Entry<String, Long> entry : _countMap.entrySet()) {
                    table.addCol(entry.getKey(), Category.getCategoryName(entry.getKey()), String.valueOf(entry.getValue()));
                }
                table.sort(Table.SORT_VALUE);
                catMap.put(statusKey,_countMap);
            }
        }

        if(true && catMap.containsKey("db_category")){
            Table catTable = report.newViewTable("cat_view_table","��Ʒ״̬�ֲ�һ��") ;
            //catTable.addCol(Report.newValue(null,"��ĿID"));
            catTable.addCol(Report.newValue(null,"һ����Ŀ"));
            catTable.addCol(Report.newValue(null,"����"));
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
