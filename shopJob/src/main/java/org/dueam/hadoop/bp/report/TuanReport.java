package org.dueam.hadoop.bp.report;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.dueam.hadoop.common.util.Fmt;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

/**
 * 服饰团购统计
 */
public class TuanReport {
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

        Report report = Report.newReport("服饰团购报表");

        Table total = report.newViewTable("total", "服饰团购 基本详情");
        Table detail = report.newTable("detail", "服饰团购  浏览详情");
        total.addCol("域名").addCol("PV").addCol("UV（会员）").addCol("聚划算PV").addCol("聚划算UV").addCol("浏览转化率").addCol(
                "PV/人").addCol(Report.BREAK_VALUE);

        Map<String, String[]> lineMap = getLineMap(input);

        String[] ls = lineMap.get("ls");

        String[] ipv = lineMap.get("ipv");

        detail.addCol("服饰团购PV", ls[1]);
        detail.addCol("服饰团购UV（会员）", ls[2]);
        detail.addCol("服饰团购UV（MID）", ls[3]);

        detail.addCol("聚划算PV", ipv[1]);
        detail.addCol("聚划算UV（会员）", ipv[2]);
        detail.addCol("聚划算UV（MID）", ipv[3]);
        detail.addCol("浏览转化率(万分比)", Fmt.parent3(ipv[2], ls[2]));

        total.addCol("服饰团购").addCol(ls[1]).addCol(ls[2]).addCol(ipv[1]).addCol(ipv[2]).addCol(
                Fmt.parent2(ipv[2], ls[2])).addCol(Fmt.div(ls[1], ls[3])).addCol(Report.BREAK_VALUE);

        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
    }

    private static Map<String, String[]> getLineMap(String input) throws IOException {

        Map<String, String[]> lineMap = new HashMap<String, String[]>();

        for (String line : Utils.readWithCharset(input, "utf-8")) {
            String[] _allCols = StringUtils.splitPreserveAllTokens(line, CTRL_A);
            lineMap.put(_allCols[0], _allCols);
        }

        return lineMap;

    }

}
