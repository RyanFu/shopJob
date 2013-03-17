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
 * Date: 11-1-6 下午5:24
 */
public class ModuleInfo {


    public static void main(String[] args) throws IOException {
        org.dueam.report.common.Report report = org.dueam.report.common.Report.newReport("七巧板模块统计报表");
        String input = args[0];
        if (!new File(input).exists()) {
            return;
        }
        List<String> today = Utils.read(input, null);
        Table table = report.newViewTable("top", "七巧板模块统计报表");
        for (String name : "模块ID,模块名称,所属市场,类目路径,小于1星卖家个数,1到5星级卖家个数,1到5钻卖家个数,1到5皇冠卖家个数,1到5金冠卖家个数,1年卖家个数,2年卖家个数,3年卖家个数,4年卖家个数,5年卖家个数,5年以上卖家个数".split(",")) {
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
