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
 * 来源统计
 */
public class ReferReport {
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
        String name = "";
        if (args.length > 1) {
            name = args[1];
        }

        Report report = Report.newReport(name + "来源情况");

        Table total = report.newViewTable("total", "基本详情");

        total.addCol("来源").addCol("PV").addCol("UV").addCol("UUV").addCol("MUV").addCol(Report.BREAK_VALUE);

        for (String line : Utils.readWithCharset(input, "utf-8")) {
            String[] cols = StringUtils.splitPreserveAllTokens(line, CTRL_A);
            if (cols.length < 5) {
                continue;
            }

            total.addCol(cols[0]);
            total.addCol(cols[0] + "pv", cols[1]);
            total.addCol(cols[0] + "uv", cols[2]);
            total.addCol(cols[0] + "uuv", cols[3]);
            total.addCol(cols[0] + "muv", cols[4]);
            total.addCol(Report.BREAK_VALUE);

        }

        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));

    }
}
