package org.dueam.hadoop.bp.report;

import org.apache.commons.lang.StringUtils;
import org.dueam.hadoop.common.util.Fmt;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.hadoop.services.Category;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * ������Ŀ�˿��ͳ��
 */
public class RefundTrade {
    private static char CTRL_A = (char) 0x01;

    /**
     * @param args
     * @throws java.io.IOException
     */
    public static void main(String[] args) throws IOException {
        String input = args[0];         //�ļ�·��
        if (!new File(input).exists()) {
            System.out.println("File Not Exist ! => " + input);
            return;
        }

        String mainName = args[1];      //��������

        Report report = Report.newReport(mainName + "�г��˿�����ͳ��");

        Table tradeTable = report.newViewTable("total_trade", mainName + "�˿�������Ϣһ����");
        tradeTable.addCol("��Ŀ").addCol("�˿��ܽ�Ԫ��").addCol("�˵���").addCol("�˵���Ԫ��").addCol(Report.BREAK_VALUE);
        for (String line : Utils.readWithCharset(input, "utf-8")) {
            String[] _cols = StringUtils.splitPreserveAllTokens(line, CTRL_A);

            String catId = _cols[1];
            if (catId == null) catId = "NULL";
            if("rootCat".equals(_cols[0])){
                tradeTable.addCol(Category.getCategoryName(catId)).addCol(Fmt.money(Long.parseLong(_cols[3]))).addCol(_cols[5]).addCol(Fmt.money(Long.parseLong(_cols[4]))).addCol(Report.BREAK_VALUE);
            }
        }
        for (String line : Utils.readWithCharset(input, "utf-8")){
            String[] _cols = StringUtils.splitPreserveAllTokens(line, CTRL_A);

            String catId = _cols[2];
            if (catId == null) catId = "NULL";
            if("leafCat".equals(_cols[0])){
                tradeTable.addCol(Category.getCategoryName(catId)).addCol(Fmt.money(Long.parseLong(_cols[3]))).addCol(_cols[5]).addCol(Fmt.money(Long.parseLong(_cols[4]))).addCol(Report.BREAK_VALUE);
            }
        }
        XmlReportFactory.dump(report, new FileOutputStream(input + ".xml"));
    }
}
