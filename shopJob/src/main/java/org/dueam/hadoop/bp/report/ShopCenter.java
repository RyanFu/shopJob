package org.dueam.hadoop.bp.report;

import org.dueam.hadoop.common.tables.BmwShops;
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

public class ShopCenter {

    static Map<String, String> totalMap = Utils.asLinkedMap(
            "all",
            "ȫ��������",
            "today",
            "�����¿�������");

    //��Ʒ���ͷֲ�
    static Map<String, String> approveStatusMap = Utils.asLinkedMap(
            "0", "����",
            "2", "Ԥ�ͷ�",
            "-1", "�ر�",
            "-2", "�ͷ�",
            "-3", "�ٷ������ʼ����һ�������õ�����",
            "-4", "�ٷ������ʼ���ڶ���������������",
            "-5", "�ٷ�����������",
            "-9", "cc");
    static Map<String, String> psMap = Utils.asLinkedMap(
            "1", "��ɫ����"
            , "2", "��Ʒ�Ƶ���"
            , "4", "ʵ������"
            , "8", "CPSȫ���ƹ���̣��Կͣ�"
            , "16", "��Ʊ�̼� "
            , "32", "�Ƶ��̼�"
            , "64", "�����̼�"
            , "128", "�����̼�"
            , "1024", "���������ε���"
            , "2048", "�����̳ǵ���");
    static Map<String, String> shopTypeMap = Utils.asLinkedMap(
            "4", "��ͨ����"
            , "1", "���̷�ֲ��"
            , "3", "���̱�׼��"
            , "7", "������չ��"
            , "2", "�����̳ǰ�"
            , "6", "���̻�Ʊ��"
            , "5", "��������"
            , "8", "������������"
            , "9", "������Ʒ");

    /**
     * @param args
     * @throws java.io.IOException
     */
    public static void main(String[] args) throws IOException {
        Report report = Report.newReport("��������");
        String input = args[0];
        if (!new File(input).exists()) {
            System.out.println("File Not Exist ! => " + input);
            return;
        }
        Map<String, List<String[]>> today = MapUtils.map(Utils.read(input, null));
        if (today == null) {
            System.out.println("No Data ! => " + input);
            return;
        }
        if (true && today.containsKey("total")) {
            Map<String, String[]> statusMap = MapUtils.toMap(today.get("total"));
            Table table = report.newTable("total", "������Ϣ����");
            for (String key : totalMap.keySet()) {
                if (null != statusMap.get(key) && statusMap.get(key).length > 0) {
                    table.addCol(key, totalMap.get(key), statusMap.get(key)[0]);
                }
            }

        }

        if (true && today.containsKey("product_count")) {
            Table table = report.newGroupTable("product_count", "������Ʒ���ֲ�");
            Map<String, String[]> statusMap = MapUtils.toMap(today.get("product_count"));

            for (String key : BmwShops.areaKeys) {
                if (null != statusMap.get(key) && statusMap.get(key).length > 0) {
                    table.addCol(key, key, statusMap.get(key)[0]);
                }
            }
        }
        if (true && today.containsKey("approve_status")) {
            Table table = report.newGroupTable("approve_status", "����״̬�ֲ�");
            Map<String, String[]> statusMap = MapUtils.toMap(today.get("approve_status"));

            for (String key : approveStatusMap.keySet()) {
                if (null != statusMap.get(key) && statusMap.get(key).length > 0) {
                    table.addCol(key, approveStatusMap.get(key), statusMap.get(key)[0]);
                }
            }
        }

        if (true && today.containsKey("site")) {
            Table table = report.newGroupTable("site", "�������ͷֲ� - ȫ��");
            Map<String, String[]> statusMap = MapUtils.toMap(today.get("site"));

            for (String key : shopTypeMap.keySet()) {
                if (null != statusMap.get(key) && statusMap.get(key).length > 0) {
                    table.addCol(key, shopTypeMap.get(key), statusMap.get(key)[0]);
                }
            }
        }

        if (true && today.containsKey("today_site")) {
            Table table = report.newGroupTable("today_site", "�������ͷֲ� - �����¿�");
            Map<String, String[]> statusMap = MapUtils.toMap(today.get("today_site"));

            for (String key : shopTypeMap.keySet()) {
                if (null != statusMap.get(key) && statusMap.get(key).length > 0) {
                    table.addCol(key, shopTypeMap.get(key), statusMap.get(key)[0]);
                }
            }
        }

        if (true && today.containsKey("promoted_status")) {
            Table table = report.newGroupTable("promoted_status", "���̱�ʶ�ֲ�");
            Map<String, String[]> statusMap = MapUtils.toMap(today.get("promoted_status"));

            for (String key : psMap.keySet()) {
                if (null != statusMap.get(key) && statusMap.get(key).length > 0) {
                    table.addCol(key, psMap.get(key), statusMap.get(key)[0]);
                }
            }
        }


        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
    }
}
