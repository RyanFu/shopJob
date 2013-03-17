package org.dueam.hadoop.bp.report;

import org.apache.commons.lang.StringUtils;
import org.dueam.hadoop.common.Area;
import org.dueam.hadoop.common.tables.BmwShops;
import org.dueam.hadoop.common.util.MapUtils;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.hadoop.services.Category;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class GoodSeller {
    private static char TAB = (char) 0x09;

    /**
     * @param args
     * @throws java.io.IOException
     */
    public static void main(String[] args) throws IOException {
        Report report = Report.newReport("各类目买家重复购买天数统计（200天）");
        String input = args[0];
        if (!new File(input).exists()) {
            System.out.println("File Not Exist ! => " + input);
            return;
        }

        Table table = report.newViewTable("all", "各类目买家重复购买天数统计");
        table.addCol(null, "类目");
        table.addCol(null, "买家数");
        table.addCol(null, "订单数");
        Area area = Area.newArea(1, 2, 5, 10, 20, 50, 100);
        for (String key : area.areaKeys) {
            table.addCol(null, key);
        }

        table.addCol(Report.newBreakValue());
        for (String line : Utils.read(input, null)) {
            String[] _allCols = StringUtils.splitPreserveAllTokens(line, TAB);
            table.addCol(null, Category.getCategoryName(_allCols[0]));
            for (int i = 1; i < _allCols.length; i++) {
                table.addCol(null, _allCols[i]);
            }
            table.addCol(Report.newBreakValue());
        }


        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
    }
}
