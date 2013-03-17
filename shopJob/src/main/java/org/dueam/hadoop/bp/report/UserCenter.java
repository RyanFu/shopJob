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

import static org.dueam.hadoop.common.Functions.str;

/**
 * User: windonly
 * Date: 11-1-6 ����5:24
 */
public class UserCenter {
    public static void main(String[] args) throws IOException {
        String input = args[0];
        if (!new File(input).exists()) {
            return;
        }
        Report report = Report.newReport("�û����ı���");
        Map<String, List<String[]>> today = MapUtils.map(Utils.read(input, null));

        if (today.get("total") != null) {
            Table table = report.newTable("total", "�û�������Ϣ");
            long sum = 0;
            for (String[] array : today.get("total")) {
                table.addCol(array[0], get(namesMap, array[0]), array[1]);
                sum += NumberUtils.toLong(array[1]);
            }
            table.addCol("sum", "�ϼ�", str(sum));
        }

        if (today.get("suspended") != null) {
            Table table = report.newGroupTable("suspended", "����ע���û�״̬�ֲ�");
            for (String[] array : today.get("suspended")) {
                table.addCol(array[0], get(suspendedMap, array[0]), array[1]);
            }
        }

        if (today.get("user_active") != null) {
            Table table = report.newGroupTable("user_active", "���켤���û����ʽ�ֲ�");
            for (String[] array : today.get("user_active")) {
                table.addCol(array[0], get(activeMap, array[0]), array[1]);
            }
        }



        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));

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
