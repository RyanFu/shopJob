package org.dueam.hadoop.bp.report;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.dueam.hadoop.common.tables.TcRefundTrade;
import org.dueam.hadoop.common.util.Fmt;
import org.dueam.hadoop.common.util.MapUtils;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.hadoop.services.Category;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * User: windonly
 * Date: 10-12-20 ����6:08
 */
public class RefundCenter {
    public static void main(String[] args) throws Exception {
        String input = args[0];
        if (!new File(input).exists()) {
            return;
        }

        Map<String, List<String[]>> today = MapUtils.map(Utils.read(input, null));
        Report report = Report.newReport("�����˿��");
        if (true && today.containsKey("total")) {
            Map<String, String> statusMap = MapUtils.toSimpleMap(today.get("total"));
            Table table = report.newTable("total", "�˿�������Ϣһ��");
            for (Map.Entry<String, String> entry : TOTAL_TABLE.entrySet()) {
                if (statusMap.containsKey(entry.getKey())) {
                    String value = statusMap.get(entry.getKey());
                    if (StringUtils.endsWith(entry.getKey(), "fee")) {
                        value = Utils.toYuan(value);
                    }
                    table.addCol(entry.getKey(), entry.getValue(), value);
                }
            }

        }

        if (true && today.containsKey("cast_time")) {
            Map<String, String> statusMap = MapUtils.toSimpleMap(today.get("cast_time"));
            Table table = report.newGroupTable("cast_time", "��������˿������ʱ��ֲ�");
            for (String key : TcRefundTrade.refundCastTimeArea.areaKeys) {
                if (statusMap.containsKey(key)) {
                    table.addCol(key, statusMap.get(key));
                }
            }

        }

        if (true && today.containsKey("cast_time_goods")) {
            Map<String, String> statusMap = MapUtils.toSimpleMap(today.get("cast_time_goods"));
            Table table = report.newGroupTable("cast_time_goods", "��������˿������ʱ��ֲ� - ���˻�");
            for (String key : TcRefundTrade.refundCastTimeArea.areaKeys) {
                if (statusMap.containsKey(key)) {
                    table.addCol(key, statusMap.get(key));
                }
            }

        }

        if (true && today.containsKey("cast_time_no_goods")) {
            Map<String, String> statusMap = MapUtils.toSimpleMap(today.get("cast_time_no_goods"));
            Table table = report.newGroupTable("cast_time_no_goods", "��������˿������ʱ��ֲ� - ���˻�");
            for (String key : TcRefundTrade.refundCastTimeArea.areaKeys) {
                if (statusMap.containsKey(key)) {
                    table.addCol(key, statusMap.get(key));
                }
            }

        }

        for (Map.Entry<String, String> group : GROUP_TABLE.entrySet()) {
            if (true && today.containsKey(group.getKey())) {
                Map<String, String> statusMap = MapUtils.toSimpleMap(today.get(group.getKey()));
                Table table = report.newGroupTable(group.getKey(), group.getValue());
                if (group.getKey().endsWith("refund_status")) {
                    for (Map.Entry<String, String> entry : REFUND_STATUS_TABLE.entrySet()) {
                        if (statusMap.containsKey(entry.getKey())) {
                            table.addCol(entry.getKey(), entry.getValue(), statusMap.get(entry.getKey()));
                        }
                    }
                }else if (group.getKey().endsWith("cs_status")) {
                    for (Map.Entry<String, String> entry : CS_STATUS_TABLE.entrySet()) {
                        if (statusMap.containsKey(entry.getKey())) {
                            table.addCol(entry.getKey(), entry.getValue(), statusMap.get(entry.getKey()));
                        }
                    }
                }
            }
        }


        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
    }


    private static Map<String, String> CS_STATUS_TABLE = MapUtils.asMap(new String[]{
            "1", "����ͷ�����",
            "2", "��Ҫ�ͷ�����",
            "3", "�ͷ��Ѿ����봦����",
            "4", "�ͷ�������� ",
            "5", "�ͷ����ܸ���ʧ��",
            "6", "�ͷ��������"
    });

    private static Map<String, String> TOTAL_TABLE = MapUtils.asMap(new String[]{
            "today_created", "�������˿��� - ���촴��",
            "today_return_fee", "��Ҫ�˿�Ľ�� - ���촴��", // ����
            "today_total_fee", "��Ҫ�˿���ܽ�� - ���촴��",
            "today_modified", "�����޸ĵļ�¼�� ",
            "all", "ȫ����¼��",
            "cast_time_sum", "������ɵ��˿����ĵ�ʱ�䣨�룩",
            "cast_time_num", "������ɵ��˿�ı���"   ,
            "cast_time_goods_sum", "������ɵ��˿����ĵ�ʱ�䣨�룩 - ���˻�",
            "cast_time_goods_num", "������ɵ��˿�ı��� - ���˻�" ,
            "cast_time_no_goods_sum", "������ɵ��˿����ĵ�ʱ�䣨�룩 - ���˻�",
            "cast_time_no_goods_num", "������ɵ��˿�ı��� - ���˻�"
    });

    private static Map<String, String> REFUND_STATUS_TABLE = MapUtils.asMap(new String[]{
            "1", "�˿�Э��ȴ�����ȷ��",
            "2", "�˿�Э���Ѿ���ɣ��ȴ�����˻�",
            "3", "������˻����ȴ�����ȷ���ջ�",
            "4", "�˿�ر�",
            "5", "�˿�ɹ�",
            "6", "���Ҳ�ͬ��Э�飬�ȴ�����޸�"
    });

    private static Map<String, String> GROUP_TABLE = MapUtils.asMap(new String[]{
            "group_last_week_refund_status", "���7��������˿�״̬ͳ��",
            "group_last_week_cs_status", "���7��������˿�ͷ�����״̬ͳ��",
            "group_refund_status", "ȫ���˿�״̬ͳ��",
            "group_cs_status", "ȫ��ͷ�����״̬ͳ��"
    });


}