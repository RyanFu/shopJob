package org.dueam.hadoop.bp.report;

import org.apache.commons.lang.math.NumberUtils;
import org.dueam.hadoop.common.Area;
import org.dueam.hadoop.common.util.MapUtils;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.hadoop.utils.uic.UicUtils;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * ׷���¿���һ�ܵ����ҵ����
 */

public class NewSellerTrace {
    public static Area ITEM_PRICE = Area.newArea(new long[]{0, 1, 2, 5, 10, 20, 50, 100, 200, 500});
    static Map<String, String> keyMap = Utils.asLinkedMap("all",
            "�¿�������",
            "rate",
            "��Щ���һ�ȡ������",
            "rate_1",
            "������",
            "rate_0",
            "������",
            "rate_-1",
            "��������"
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
        Report report = Report.newReport("һ��ǰ��������ҽ������׷��");
        String input = args[0];
        if (!new File(input).exists()) {
            System.out.println("File Not Exist ! => " + input);
            return;
        }
        Map<String, List<String[]>> today = MapUtils.map(Utils.readWithCharset(input, "utf-8"));
        if (today == null) {
            System.out.println("No Data ! => " + input);
            return;
        }
        if (true && today.containsKey("total")) {
            Map<String, String[]> statusMap = MapUtils.toMap(today.get("total"));
            if (true) {
                Table table = report.newTable("total", "��������");
                for (String key : keyMap.keySet()) {
                    if (null != statusMap.get(key) && statusMap.get(key).length > 0) {
                        table.addCol(key, keyMap.get(key), statusMap.get(key)[0]);
                    }
                }
            }


        }

        if (true && today.containsKey("rate")) {
            Table table = report.newGroupTable("rate", "�û������õȼ��ֲ�");
            Map<String, String> statusMap = MapUtils.toSimpleMap(today.get("rate"));
            for (String key : UicUtils.allRate) {
                if (statusMap.containsKey(key))
                    table.addCol(String.valueOf(UicUtils.getRankPos(key)), key, statusMap.get(key));
            }
        }

        if (true && today.containsKey("item_price")) {
            Table table = report.newGroupTable("item_price", "���������Ʒ�ļ۸�ֲ����䣨��λ��Ԫ��");
            Map<String, String> statusMap = MapUtils.toSimpleMap(today.get("item_price"));
            for (String key : ITEM_PRICE.areaKeys) {
                if (statusMap.containsKey(key)) {
                    table.addCol(key, statusMap.get(key));
                }
            }

        }

        if (true && today.containsKey("seller") && today.containsKey("seller_and_buyer")) {
            Table table = report.newGroupTable("rate_avg", "�����յ���ÿ�����ƽ���������ֲ�");

            Map<String, String> sellerMap = MapUtils.toSimpleMap(today.get("seller"));
            Map<String, List<String[]>> sellerAndBuyerMap = MapUtils.map(today.get("seller_and_buyer"));

            Area area = Area.newArea(1, 2, 3, 4, 5, 10, 20, 50);
            for (Map.Entry<String, String> entry : sellerMap.entrySet()) {
                if (sellerAndBuyerMap.containsKey(entry.getKey())) {
                    double avg = NumberUtils.toDouble(entry.getValue()) / sellerAndBuyerMap.get(entry.getKey()).size();
                    area.count(avg);
                }
            }

            for (String key : area.areaKeys) {
                Long value = area.get(key);
                if (value != null) {
                    table.addCol(key, String.valueOf(value));
                }
            }
        }
        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
    }


}
