package org.dueam.hadoop.bp.report;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.dueam.hadoop.common.util.MapUtils;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

/**
 *双12活动页面
 */
public class D12HdymReport {
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

        Map<String, List<String[]>> today = MapUtils.map(Utils.read(input), CTRL_A);

        Map<String, String[]> lsMap = MapUtils.toMap(today.get("ls"));
        //  Map<String, String[]> ipvMap = MapUtils.toMap(today.get("ipv"));

        Report report = Report.newReport(name + "统计");

        Map<String, Table> marketMap = new HashMap<String, Table>();

        for (Entry<String, String[]> entry : lsMap.entrySet()) {

            String key = entry.getKey();//eg: 针织-针织衫市场

            String[] ls = entry.getValue();//value[0]:pv,value[1]:uv,value[2]:mid;

            String[] keys = StringUtils.split(key, "-");

            Table market = marketMap.get(keys[1]);

            if (market == null) {
                market = report.newViewTable(keys[1], keys[1]);
                market.addCol("活动页面").addCol("PV").addCol("UV").addCol("UV(MID)").addCol(Report.BREAK_VALUE);
                marketMap.put(keys[1], market);
            }
            market.addCol(keys[0]).addCol(ls[0]).addCol(ls[1]).addCol(ls[2]).addCol(Report.BREAK_VALUE);
        }

        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
    }
}
