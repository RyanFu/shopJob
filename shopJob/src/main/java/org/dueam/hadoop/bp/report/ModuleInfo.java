package org.dueam.hadoop.bp.report;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.dueam.hadoop.common.tables.RFactSellPV;
import org.dueam.hadoop.common.util.Fmt;
import org.dueam.hadoop.common.util.MapUtils;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.report.common.Report;
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
public class ModuleInfo {


    public static void main(String[] args) throws IOException {
        org.dueam.report.common.Report report = org.dueam.report.common.Report.newReport("���ɰ�ģ��ͳ�Ʊ���");
        String input = args[0];
        if (!new File(input).exists()) {
            return;
        }
        List<String> today = Utils.read(input, null);
        Table table = report.newViewTable("top", "���ɰ�ģ��ͳ�Ʊ���");
        for (String name : "ģ��ID,ģ������,�����г�,��Ŀ·��,С��1�����Ҹ���,1��5�Ǽ����Ҹ���,1��5�����Ҹ���,1��5�ʹ����Ҹ���,1��5������Ҹ���,1�����Ҹ���,2�����Ҹ���,3�����Ҹ���,4�����Ҹ���,5�����Ҹ���,5���������Ҹ���".split(",")) {
            table.addCol(null, name);

        }
        table.addCol(Report.newBreakValue());

        for (String line : today) {
            String[] _allCols = StringUtils.splitPreserveAllTokens(line, ',');
            int pos = 0;
            for (String col : _allCols) {
                col = StringUtils.strip(col,"\"");
                if (pos > 3 && false) {
                    table.addCol(_allCols[0] + "_" + pos, col);
                } else {
                    table.addCol(null, col);
                }
                pos++;

            }
            table.addCol(Report.newBreakValue());

        }

        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));

    }
}
