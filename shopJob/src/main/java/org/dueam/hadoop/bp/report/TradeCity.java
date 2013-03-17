package org.dueam.hadoop.bp.report;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.dueam.hadoop.common.util.Fmt;
import org.dueam.hadoop.common.util.MapUtils;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.hadoop.services.Category;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static org.dueam.hadoop.common.Functions.newHashMap;

/**
 * ������Ŀ������ά��ͳ�ƽ��׶�
 */
public class TradeCity {
    private static char CTRL_A = (char) 0x01;

    /**
     * @param args
     * @throws java.io.IOException
     */
    public static void main(String[] args) throws IOException {
        String input = args[0];         //�ļ�·��
        String mainName = args[1];      //��������

        if (!new File(input).exists()) {
            System.out.println("File Not Exist ! => " + input);
            return;
        }

        Report report = Report.newReport(mainName + "�г����ҵ������");

        Table tradeTable = report.newTable("trade_table", mainName + "�г���������");
        Table orderTable = report.newTable("order_table", mainName + "�г���������");

        for (String line : Utils.readWithCharset(input, "utf-8")){
            String[] _cols = StringUtils.splitPreserveAllTokens(line, CTRL_A);
            tradeTable.addCol(_cols[0] , _cols[0], _cols[1]);
            orderTable.addCol(_cols[0] , _cols[0], _cols[2]);
        }
        tradeTable.sort(Table.SORT_VALUE);
        orderTable.sort(Table.SORT_VALUE);

        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
    }


}
