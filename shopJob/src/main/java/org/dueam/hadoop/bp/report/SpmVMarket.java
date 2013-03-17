package org.dueam.hadoop.bp.report;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.dueam.hadoop.common.CounterMap;
import org.dueam.hadoop.common.util.MapUtils;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.hadoop.services.ShopDomain;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.commons.lang.math.NumberUtils.toInt;
import static org.apache.commons.lang.math.NumberUtils.toLong;
import static org.dueam.hadoop.common.Functions.str;

/**
 * ����ҳ����ͳ��
 */
public class SpmVMarket {
    private static char CTRL_A = (char) 0x01;

    /**
     * @param args
     * @throws java.io.IOException
     */
    public static void main(String[] args) throws IOException {
        String input = args[0];
        //String input = "20110923";
        if (!new File(input).exists()) {
            System.out.println("File Not Exist ! => " + input);
            return;
        }

        Report report = Report.newReport("����&��ɫ��ҳ����ͳ��");

        Map<String, List<String[]>> today = MapUtils.map(Utils.read(input, (String[]) null), CTRL_A);
        if (today.containsKey("module")) {
            Table table = report.newViewTable("total", "����ģ����ͳ��");
            table.addCol("ģ������").addCol("PV").addCol("UPV").addCol("UV").addCol("UUV").breakRow();
            Map<String, String[]> statusMap = MapUtils.toMap(today.get("module"));
            for (Map.Entry<String, String> entry : KEYS.entrySet()) {
                if (statusMap.containsKey(entry.getKey())) {
                    String[] _cols = statusMap.get(entry.getKey());
                    table.addCol(entry.getValue());
                    table.addCol(entry.getKey() + "-pv", str(toLong(_cols[0])));
                    table.addCol(entry.getKey() + "-upv", str(toLong(_cols[1])));
                    table.addCol(entry.getKey() + "-uv", str(toLong(_cols[2])));
                    table.addCol(entry.getKey() + "-uuv", str(toLong(_cols[3])));
                    table.breakRow();
                }
            }
        }
        if (today.containsKey("position")) {
            Map<String, List<String[]>> statusMap = MapUtils.map(today.get("position"));

            for (Map.Entry<String, String> entry : KEYS.entrySet()) {
                if (statusMap.containsKey(entry.getKey())) {
                    Table table = report.newViewTable("detail_" + entry.getKey(), entry.getValue() + "ģ��������");
                    table.addCol("���λ��").addCol("PV").addCol("UPV").addCol("UV").addCol("UUV").breakRow();
                    List<String[]> _colsList = statusMap.get(entry.getKey());
                    Collections.sort(_colsList, new Comparator<String[]>() {
                        public int compare(String[] o1, String[] o2) {
                            return toInt(o1[0]) - toInt(o2[0]);
                        }
                    });

                    for (String[] _cols : _colsList) {
                        String key = entry.getKey() + "." + _cols[0];
                        table.addCol(key);
                        table.addCol(key + "-pv", str(toLong(_cols[1])));
                        table.addCol(key + "-upv", str(toLong(_cols[2])));
                        table.addCol(key + "-uv", str(toLong(_cols[3])));
                        table.addCol(key + "-uuv", str(toLong(_cols[4])));
                        table.breakRow();
                    }
                }
            }
        }

        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
    }

    static Map<String, String> KEYS = MapUtils.asMap(("10\n" +
            "�г��л�\n" +
            "11\n" +
            "Ůװ����ѡ������ͷ��\n" +
            "12\n" +
            "������ʾ��\n" +
            "13\n" +
            "������\n" +
            "14\n" +
            "����ѡ��\n" +
            "15\n" +
            "��Ʒ�б��ͼƬ���\n" +
            "16\n" +
            "������̵��\n" +
            "17\n" +
            "�������۵��\n" +
            "18\n" +
            "ҳ�׷�ҳͳ��\n" +
            "19\n" +
            "�������ģʽ\n" +
            "20\n" +
            "������ͼ���첽����\n" +
            "22\n" +
            "ҳ������չ�����ض���\n" +
            "23\n" +
            "�������ģʽ\n" +
            "24\n" +
            "������").split("\n"));
}
