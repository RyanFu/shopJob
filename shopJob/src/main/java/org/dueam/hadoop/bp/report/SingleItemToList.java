package org.dueam.hadoop.bp.report;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.commons.lang.StringUtils;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

/**
 * list�ĵ�Ʒ��������Դ���
 */
public class SingleItemToList {
    private static char CTRL_A = (char) 0x01;

    /**
     * @param args
     * @throws java.io.IOException
     */
    public static void main(String[] args) throws IOException {
        String input = args[0];
        if (!new File(input).exists()) {
            System.out.println("File Not Exist ! => " + input);
            return;
        }
        Report report = Report.newReport("��Ʒ��������Դ���");
        Table singleItemPv = report.newGroupTable("singleItemPv", "��Ʒ��������Դ��pvͳ�Ʊ���");
        Table singleItemUv = report.newGroupTable("singleItemUv", "��Ʒ��������Դ��uvͳ�Ʊ���");
        for (String line : Utils.readWithCharset(input, "utf-8")) {
            String[] _allCols = StringUtils.splitPreserveAllTokens(line, CTRL_A);
            if (_allCols.length < 2) continue;
            singleItemPv.addCol(_allCols[0], _allCols[1]);
            singleItemUv.addCol(_allCols[0], _allCols[2]);
        }
        singleItemPv.sort(Table.SORT_VALUE);
        singleItemUv.sort(Table.SORT_VALUE);
        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
    }
}
