package org.dueam.hadoop.bp.report;

import org.apache.commons.lang.math.NumberUtils;
import org.dueam.hadoop.common.util.ItemUtils;
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

/**
 * User: windonly
 * Date: 11-1-6 ����5:24
 */
public class Auctions {
    public static void main(String[] args) throws IOException {
        String input = args[0];
        if (!new File(input).exists()) {
            return;
        }
        Report report = Report.newReport("��������׷�ٱ���");
        Map<String, List<String[]>> today = MapUtils.map(Utils.read(input, null));
        if (today.get("total") != null) {
            Table table = report.newTable("total","ȫ����������������Ϣ");
            for (String[] array : today.get("total")) {
                if (NumberUtils.isNumber(array[0])) {
                    table.addCol(array[0], ItemUtils.getItemStatusName(array[0]), array[1]);
                } else {
                    table.addCol(array[0], getName(array[0]), array[1]);
                }
            }
            today.remove("total");
        }
        for (Map.Entry<String, List<String[]>> entry : today.entrySet()) {
            Table table = report.newTable("today_"+entry.getKey(),getName(entry.getKey())+"����������Ϣ");
            for (String[] array : entry.getValue()) {
                table.addCol(array[0],getName(array[0]), array[1]);
            }

        }
        XmlReportFactory.dump(report,new FileOutputStream(args[0] + ".xml"));

    }

    public static String getName(String key) {
        String value = namesMap.get(key);
        if (value != null) return value;
        return key;
    }

    static Map<String, String> namesMap = Utils.asMap("all,ȫ��,many,������,one,������,publish,���췢��,start,�����ϼ�,ends,�����¼�,ends_unbid,�����¼ܣ�δ���ۣ�,ends_bid,�����¼ܣ��ѳ��ۣ�,bid,�Ѿ�����,unbid,��δ����".split(","));
}
