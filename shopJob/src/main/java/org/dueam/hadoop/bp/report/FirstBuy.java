package org.dueam.hadoop.bp.report;

import org.apache.commons.lang.StringUtils;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

/**
 * 各个类目按城市维度统计交易额
 */
public class FirstBuy {
    private static char CTRL_A = (char) 0x01;

    /**
     * @param args
     * @throws java.io.IOException
     */
    public static void main(String[] args) throws IOException {
        String input    = args[0];      //文件路径
        String mainName = args[1];      //报表名称

        if (!new File(input).exists()) {
            System.out.println("File Not Exist ! => " + input);
            return;
        }

        Report report = Report.newReport(mainName + "市场每日新增用户数据");

        Table tradeTable = report.newTable("first_buy", mainName + "市场每日新增购买用户数据");
        Table orderCountTable = report.newTable("order_count", mainName + "市场总订单数");

        //for (String line : Utils.readWithCharset(input, "utf-8")){
            //String[] _cols = StringUtils.splitPreserveAllTokens(line, CTRL_A);
            //tradeTable.addCol(_cols[0] , _cols[0], _cols[1]);
        //}
        List<String> lines = Utils.readWithCharset(input, "utf-8");
        if(lines != null && lines.size()>0){
            tradeTable.addCol("count" , "每日新增购买人数", lines.get(0));

            String[] _cols = StringUtils.splitPreserveAllTokens(lines.get(1), CTRL_A);
            orderCountTable.addCol("orderCount" , "每日总订单数", _cols[1]);
        }
        tradeTable.sort(Table.SORT_VALUE);
        orderCountTable.sort(Table.SORT_VALUE);

        XmlReportFactory.dump(report, new FileOutputStream(input + ".xml"));
    }


}
