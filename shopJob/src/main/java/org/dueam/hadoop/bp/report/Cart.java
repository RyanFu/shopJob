package org.dueam.hadoop.bp.report;

import org.apache.commons.lang.math.NumberUtils;
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
public class Cart {
    private static Map<String,String> todayOrder = Utils.asLinkedMap("last","�������һ�μ��빺�ﳵ���µ�","created","�����һ�μ��빺�ﳵ���µ�");

    public static void main(String[] args) throws IOException {
        org.dueam.report.common.Report report = org.dueam.report.common.Report.newReport("���ﳵ׷�ٱ���");
        String input = args[0];
        if (!new File(input).exists()) {
            return;
        }
        Map<String, List<String[]>> today = MapUtils.map(Utils.read(input, null));
        if (true) {

            Table table = report.newTable("cart", "���ﳵ������Ϣ", "γ��", "����");
            for (String[] array : today.get("sum")) {
                table.addCol(org.dueam.report.common.Report.newValue(array[0], array[0], array[1]));
            }

            for (String[] array : today.get("today_order")) {
                table.addCol(org.dueam.report.common.Report.newValue("today_order_"+array[0], todayOrder.get(array[0]), array[1]));

            }



            for (String[] array : today.get("del")) {
                table.addCol(org.dueam.report.common.Report.newValue(array[0], status(array[0]), array[1]));
            }

            if (today.containsKey("avg_order")) {
                long sum = 0;
                long count = 0;
                for (String[] array : today.get("avg_order")) {
                    if ("sum".equals(array[0])) {
                        sum = NumberUtils.toLong(array[1]);
                    } else if ("count".equals(array[0])) {
                        count = NumberUtils.toLong(array[1]);
                    }
                }

                table.addTimeCol("avg_order", "���뵽�µ�ƽ��ʱ��(��)", Fmt.divNumber(sum, count));
            }

            if (today.containsKey("avg_last")) {
                long sum = 0;
                long count = 0;
                for (String[] array : today.get("avg_last")) {
                    if ("sum".equals(array[0])) {
                        sum = NumberUtils.toLong(array[1]);
                    } else if ("count".equals(array[0])) {
                        count = NumberUtils.toLong(array[1]);
                    }
                }
                 table.addTimeCol("avg_last", "����޸ĵ��µ�ƽ��ʱ��(��)", Fmt.divNumber(sum, count));
            }
        }

        if (true) {
            Table table = report.newGroupTable("GMT_MODIFIED-GMT_CREATE", "��Ʒ��һ�μ��빺�ﳵ���µ���ʱ��ֲ�(GMT_MODIFIED - GMT_CREATE)", "γ��", "����");
            Map<String, String> map = new HashMap<String, String>();
            long sum = 0;
            for (String[] array : today.get("order")) {
                map.put(array[0], array[1]);
                sum += NumberUtils.toLong(array[1], 0);
            }
            for (String key : org.dueam.hadoop.conf.Cart.areaKeys) {
                table.addCol(org.dueam.report.common.Report.newValue(key, map.get(key)));
            }
            table.setSummary(table.getSummary());
        }

        if (true && today.get("last") != null && !today.get("last").isEmpty()) {
            Table table = report.newGroupTable("GMT_MODIFIED - LAST_MODIFIED", "��Ʒ���һ�μ��빺�ﳵ���µ���ʱ��ֲ�(GMT_MODIFIED - LAST_MODIFIED)", "γ��", "����");
            Map<String, String> map = new HashMap<String, String>();
            long sum = 0;
            for (String[] array : today.get("last")) {
                map.put(array[0], array[1]);
                sum += NumberUtils.toLong(array[1], 0);
            }
            for (String key : org.dueam.hadoop.conf.Cart.areaKeys) {
                table.addCol(org.dueam.report.common.Report.newValue(key, map.get(key)));
            }
            table.setSummary(table.getSummary());
        }

        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));

    }
}
