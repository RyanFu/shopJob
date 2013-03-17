package org.dueam.hadoop.bp.report;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.dueam.hadoop.common.util.Fmt;
import org.dueam.hadoop.common.util.MapUtils;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.hadoop.services.Category;
import org.dueam.hadoop.utils.uic.UicUtils;
import org.dueam.hadoop.utils.uic.UserDO;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * User: windonly
 * Date: 10-12-20 ����6:08
 */
public class BuyerTradeCounterMonitor {
    public static void main(String[] args) throws Exception {
        String input = args[0];
        if (!new File(input).exists()) {
            return;
        }
        Report report = Report.newReport("����Ҽ��");
        Table table = report.newViewTable("total", "���90�칺��������2W(������)������б�");
        table.addCol(null, "USER_ID");
        table.addCol(null, "�ǳ�");
        table.addCol(null, "״̬");
        table.addCol(null, "�ܶ�����");
        table.addCol(null, "������");
        table.addCol(null, "��ȷ���ջ�");
        table.addCol(null, "������");
        table.addCol(Report.newBreakValue());
        for (String line : Utils.read(input)) {
            String[] _cols = StringUtils.splitPreserveAllTokens(line, Utils.TAB);
            UserDO user = UicUtils.getUser(_cols[0]);
            //System.out.println(user);
            table.addCol(null, _cols[0],_cols[0]);
            table.addCol(null, user.getNick(), user.getNick());
            table.addCol(null, null, user.getSuspendedStatus());
            table.addCol(null, "�ܶ�����", _cols[1]);
            table.addCol(null, "������", _cols[2]);
            table.addCol(null, "��ȷ���ջ�", _cols[3]);
            table.addCol(null, "������", _cols[4]);
            table.addCol(Report.newBreakValue());
        }


        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
    }


}