package org.dueam.hadoop.bp.report;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.dueam.hadoop.common.util.Fmt;
import org.dueam.hadoop.common.util.MapUtils;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

/**
 * list分类情况,类目,地区等
 */
public class ListClassifyReport {
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

        Report report = Report.newReport(name + "list情况");

        Map<String, List<String[]>> today = MapUtils.map(Utils.readWithCharset(input, "utf-8"), CTRL_A);

        Table total = report.newViewTable("total", "基本详情");

        total.addCol("类别").addCol("PV").addCol("UV").addCol("UUV").addCol("MUV").addCol("人均PV").addCol("浏览转化率").addCol(
                "购买转化率").addCol(Report.BREAK_VALUE);

        Map<String, String[]> lsMap = MapUtils.toMap(today.get("ls"));
        Map<String, String[]> ipvMap = MapUtils.toMap(today.get("ipv"));
        Map<String, String[]> tradeMap = MapUtils.toMap(today.get("trade"));

        for (Entry<String, String[]> entry : lsMap.entrySet()) {

            String key = entry.getKey();
            String[] val = entry.getValue();

            total.addCol(key);
            total.addCol(key + "pv", val[0]);
            total.addCol(key + "uv", val[1]);
            total.addCol(key + "uuv", val[2]);
            total.addCol(key + "muv", val[3]);

            total.addCol(key + "avg-pv", Fmt.div(val[0], val[1]));

            if (ipvMap.get(key) == null) {
                total.addCol(key + "view", "-");
                total.addCol(key + "buy", "-");
                total.addCol(Report.BREAK_VALUE);
                continue;
            }
            total.addCol(key + "view", Fmt.parent5(ipvMap.get(key)[1], val[1]));

            if (tradeMap.get(key) == null) {
                total.addCol(key + "buy", "-");
                total.addCol(Report.BREAK_VALUE);
                continue;
            }
            total.addCol(key + "buy", Fmt.parent5(tradeMap.get(key)[2], ipvMap.get(key)[1]));
            total.addCol(Report.BREAK_VALUE);

        }

        XmlReportFactory.dump(report, new FileOutputStream(input + ".xml"));

    }

}
