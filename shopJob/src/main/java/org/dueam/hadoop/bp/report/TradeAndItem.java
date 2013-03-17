package org.dueam.hadoop.bp.report;

import org.apache.commons.lang.math.NumberUtils;
import org.dueam.hadoop.common.util.MapUtils;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.Value;
import org.dueam.report.common.XmlReportFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * ����Ŀ������Ч��
 * User: windonly
 * Date: 11-1-6 ����5:24
 */
public class TradeAndItem {
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.out.println("args : 20110331 ../trade_center/20110331.xml ../item_center/20110331.xml ");
            return;
        }
        String data = args[0];
        String trade = args[1];
        String item = args[2];
        if (!new File(trade).exists() || !new File(item).exists()) {
            System.out.println("trade and item not exists !" + trade + item);
            return;
        }

        Report tradeReport = XmlReportFactory.load(new FileInputStream(trade));
        Report itemReport = XmlReportFactory.load(new FileInputStream(item));
        Table tradeTable = tradeReport.getTable("cat");
        Table itemTable = itemReport.getTable("db_category");
        if (itemTable == null || tradeTable == null) {
            System.out.println(tradeReport.getTables());
            System.out.println(itemReport.getTables());
            return;
        }

        tradeTable.sort(Table.SORT_VALUE);

        Report report = Report.newReport("��Ŀ��Ʒ������");
        if (true) {
            Table table = report.newTable("tradeAndItem","��Ŀ��Ʒ��ֵ����Ŀ���׶�֣�/��Ŀ��Ʒ����");
            Map<String,Long> itemMap = new HashMap<String,Long>();
            for(Value value : itemTable.getValues()){
                itemMap.put(value.getKey(), NumberUtils.toLong(value.getValue()));
            }
            for(Value value : tradeTable.getValues()){
                if(itemMap.containsKey(value.getKey()) && itemMap.get(value.getKey())>0){
                    long tradeAndItem = value.getValueAsLong() * 100 / itemMap.get(value.getKey());
                    table.addCol(value.getKey(),value.getName(),String.valueOf(tradeAndItem));
                }

            }
        }

        //��ӽ�������Ŀ����
        if (true) {

            report.putTable(tradeTable);
            itemTable.sort(Table.SORT_VALUE);
            report.putTable(itemTable);
        }
        XmlReportFactory.dump(report, new FileOutputStream(data + ".xml"));

    }


    public static String get(Map<String, String> map, String key) {
        String value = map.get(key);
        if (value != null) return value;
        return key;
    }

    static Map<String, String> namesMap = Utils.asMap("reg,ע���û�,active,�����û�".split(","));
    static Map<String, String> suspendedMap = Utils.asMap("0,����,1,δ����,2,ɾ��,3,����,-9,CC".split(","));
    static Map<String, String> activeMap = Utils.asMap("1,�ʼ�,2,�ֻ�,3,�ֻ����ʼ�,4,�����ʼ��ϵ�У����,7,�⼤��,8,����ע��,11,�ֻ�ע��,12,wap ע��Ļ�Ա,13,�Ա���Ʊע��,14,ע��С���˺�,15,wap ����ע��,16,΢֧���Զ�ע��,17,�°�֧��������Q���⼤�".split(","));
}
