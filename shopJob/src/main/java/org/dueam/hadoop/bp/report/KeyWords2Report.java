package org.dueam.hadoop.bp.report;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.dueam.hadoop.common.util.Fmt;
import org.dueam.hadoop.common.util.MapUtils;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

/**
 * search引导到垂直市场report
 */
public class KeyWords2Report {
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

        String name = "defult";
        if (args.length > 1) {
            name = args[1];
        }

        Map<String, List<String[]>> today = MapUtils.map(Utils.read(input), CTRL_A);

        Map<String, String[]> lsMap = MapUtils.toMap(today.get("ls"));
        Map<String, String[]> ipvMap = MapUtils.toMap(today.get("ipv"));

        Report report = Report.newReport(name + "引导报表");

        Table market = report.newViewTable(name, name);

        market.addCol("市场").addCol("关键词").addCol("SEARCH-PV").addCol("SEARCH-UV").addCol("引导-PV").addCol("引导-UV")
                .addCol("转化率").addCol(Report.BREAK_VALUE);

        for (Entry<String, String[]> entry : lsMap.entrySet()) {

            String key = entry.getKey();//eg: 针织-针织衫市场

            String[] ls = entry.getValue();//value[0]:pv,value[1]:uv,value[2]:mid;

            String[] keys = StringUtils.split(key, "-");

            String[] ipv = ipvMap.get(key);

            if (ipv == null) {
                ipv = new String[] { "0", "0", "0" };
            }

            market.addCol(keys[1]).addCol(keys[0]).addCol(ls[0]).addCol(ls[1]).addCol(ipv[0]).addCol(ipv[1]).addCol(
                    Fmt.parent2(ipv[1], ls[1])).addCol(Report.BREAK_VALUE);

        }

        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
    }
}
