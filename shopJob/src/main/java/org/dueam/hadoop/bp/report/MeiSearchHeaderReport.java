package org.dueam.hadoop.bp.report;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

/**
 * 搜美妆市场统计
 * 
 * @author linyi
 */
public class MeiSearchHeaderReport {
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

        Report report = Report.newReport("搜美妆市场统计");

        Table summay = report.newTable("summay", "基本详情");

        for (String line : Utils.readWithCharset(input, "utf-8")) {
            String[] allCols = StringUtils.splitPreserveAllTokens(line, CTRL_A);

            if (HOSTS.contains(allCols[0])) {
                summay.addCol(allCols[0] + "-PV", allCols[1]);
                summay.addCol(allCols[0] + "-UV", allCols[2]);
                summay.addCol(allCols[0] + "-MID", allCols[3]);
            }

        }

        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
    }

    private static List<String> HOSTS = Arrays.asList(new String[] { "list.taobao.com", "mei.taobao.com", });

}
