package org.dueam.hadoop.bp.report;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * 女装卖家统计
 */
public class HostPvUvTo {
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

        Report report = Report.newReport(args[1]);
        long minNum = 5;
        Table tablePv = report.newGroupTable("pv", "PV统计");
        Table tableUv = report.newGroupTable("uv", "UV统计");

        for (String line : Utils.readWithCharset(input, "utf-8")) {
            String[] _allCols = StringUtils.splitPreserveAllTokens(line, CTRL_A);
            if (_allCols.length < 2) continue;
            String key = _allCols[0];
            if (StringUtils.isEmpty(key)) key = "-";
            if (NumberUtils.toLong(_allCols[1]) > minNum) {
                tablePv.addCol(key, _allCols[1]);
                tableUv.addCol(key, _allCols[2]);
            }
        }
        tablePv.sort(Table.SORT_VALUE);
        tableUv.sort(Table.SORT_VALUE);

        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
    }
}
