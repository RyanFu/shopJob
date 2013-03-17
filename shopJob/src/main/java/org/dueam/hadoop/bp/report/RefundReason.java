package org.dueam.hadoop.bp.report;

import org.dueam.hadoop.common.util.MapUtils;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.hadoop.services.Category;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.util.List;
import java.util.Map;

/**
 * ����Ŀԭ��ͳ��
 * User: windonly
 * Date: 10-12-20 ����6:08
 */
public class RefundReason {
    static char CTRL_A = (char) 0x01;

    public static void main(String[] args) throws Exception {
        String input = args[0];
        if (!new File(input).exists()) {
            return;
        }

        Map<String, List<String[]>> today = MapUtils.map(Utils.read(input, null), CTRL_A);
        Report report = Report.newReport("������Ŀ���˿�ԭ��ͳ�ƣ����30�죩");
        for (Map.Entry<String, List<String[]>> entry : today.entrySet()) {
            String name = Category.getCategoryName(entry.getKey()) + "��Ŀ���˿�ԭ��ֲ�";
            Table table1 = report.newGroupTable(entry.getKey() + "_1", name + "(���˻�)");
            Table table2 = report.newGroupTable(entry.getKey() + "_0", name + "(�����˻�)");
            for (String[] _cols : entry.getValue()) {
                if ("1".equals(_cols[0])) {
                    table1.addCol(_cols[1], REASONS.get(_cols[1]), _cols[2]);
                } else {
                    table2.addCol(_cols[1], REASONS.get(_cols[1]), _cols[2]);
                }
            }
        }


        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
    }


    private static Map<String, String> REASONS = MapUtils.asMap("1,δ�յ�����,2,����ȱ��,3,�Ĵ���Ʒ,4,��Ʒȱ��������ʽ,5,������Э��һ���˿�,6,����δ��ʱ����\t,11,�����������˻���,12,��Ʒ��������,13,�յ�����Ʒ����,14,δ�յ���,15,�˻��ʷ�,16,�ۿۡ���Ʒ����Ʊ����,18,�յ��ٻ�\t,19,������Э��һ���˿�,20,������˾��������,21,������ٷ���,22,����û����24Сʱ�ڷ���,0,����".split(","));


}