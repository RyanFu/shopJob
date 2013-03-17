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
 * 各类目贡献率效率
 * User: windonly
 * Date: 11-1-6 下午5:24
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

        Report report = Report.newReport("类目产品贡献力");
        if (true) {
            Table table = report.newTable("tradeAndItem","类目商品价值（类目交易额（分）/类目商品数）");
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

        //添加交易与类目报表
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

    static Map<String, String> namesMap = Utils.asMap("reg,注册用户,active,激活用户".split(","));
    static Map<String, String> suspendedMap = Utils.asMap("0,正常,1,未激活,2,删除,3,冻结,-9,CC".split(","));
    static Map<String, String> activeMap = Utils.asMap("1,邮件,2,手机,3,手机、邮件,4,输入邮件上的校验码,7,免激活,8,快速注册,11,手机注册,12,wap 注册的会员,13,淘宝机票注册,14,注册小二账号,15,wap 短信注册,16,微支付自动注册,17,新版支付宝快速Q（免激活）".split(","));
}
