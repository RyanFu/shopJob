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
 * Date: 11-1-6 ����5:24
 */
public class SkuCenter {
    public static void main(String[] args) throws IOException {
        org.dueam.report.common.Report report = org.dueam.report.common.Report.newReport("SKU���ͳ�Ʊ���");
        String input = args[0];
        if (!new File(input).exists()) {
            return;
        }
        Map<String, List<String[]>> today = MapUtils.map(Utils.read(input, null));
        if (true && today.containsKey("sum")) {
            Map<String,String> map = MapUtils.toSimpleMap(today.get("sum"));
            Table table = report.newTable("sum", "����ָ��", "γ��", "����");
            table.addCol("sku","�ܼ�¼��",map.get("sku"));
            table.addCol("sku_status_1","��¼����������",map.get("sku_status_1"));
            table.addCol("sku_status_-1","��¼����ɾ����",map.get("sku_status_-1"));
            table.addCol("item_sum","SKU������������",map.get("item"));
            table.addCol("item_count","��Ʒ����������",map.get("item_count"));
            table.addCol("outer_sum","SKU���������� - ���̼ұ��룩",map.get("outer"));
            table.addCol("outer_count","�̼ұ�������",map.get("outer_count"));
            table.addCol("created","���촴���ļ�¼��",map.get("created"));
            table.addCol("modified","�����޸ĵļ�¼��",map.get("modified"));
        }

        if (true && today.containsKey("item")) {
            Table table = report.newGroupTable("item_area", "������Ʒ��SKU�����ֲ�", "γ��", "����");
            Map<String,String> map = MapUtils.toSimpleMap(today.get("item"));
            for (String key : Sku.areaKeys) {
                if(StringUtils.isNotEmpty(map.get(key))){
                    table.addCol(key,map.get(key));
                }
            }
        }


        if (true && today.containsKey("outer")) {
            Table table = report.newGroupTable("outer_area", "�����̼ұ�����SKU�����ֲ�", "γ��", "����");
            Map<String,String> map = MapUtils.toSimpleMap(today.get("outer"));
            for (String key : Sku.areaKeys) {
                if(StringUtils.isNotEmpty(map.get(key))){
                    table.addCol(key,map.get(key));
                }
            }
        }

        if (true && today.containsKey("seller")) {
            Table table = report.newGroupTable("seller_area", "��������������̼ұ��������ֲ�", "γ��", "����");
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
