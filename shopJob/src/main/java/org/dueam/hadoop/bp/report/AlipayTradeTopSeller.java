package org.dueam.hadoop.bp.report;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.dueam.hadoop.common.util.Fmt;
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

/**
 * User: windonly
 * Date: 10-12-20 ����6:08
 */
public class AlipayTradeTopSeller {
    public static void main(String[] args) throws Exception {
        String input = args[0];
        if (!new File(input).exists()) {
            return;
        }
        Report report = Report.newReport("ÿ��֧��������Top����");
        Table table = report.newViewTable("total", "ÿ��֧��������Top 200 ����");
        //table.addCol(null, "����ID");
        table.addCol(null, "�ǳ�");
        table.addCol(null, "�̳�");
        table.addCol(null, "���׶�");
        table.addCol(null, "���׶�");
        table.addCol(null, "���ױ���");
        table.addCol(null, "��Ӫ��Ŀ");
        table.addCol(Report.newBreakValue());
        List<String> lines = Utils.read(input);
        Collections.sort(lines, new Comparator<String>() {
            public int compare(String o1, String o2) {
                long trade1 = NumberUtils.toLong(StringUtils.splitPreserveAllTokens(o1, Utils.TAB)[2]);
                long trade2 = NumberUtils.toLong(StringUtils.splitPreserveAllTokens(o2, Utils.TAB)[2]);
                if (trade1 == trade2) return 0;
                return trade1 > trade2 ? -1 : 1;
            }
        });
        int count = 0;
        int maxCount = 500;
        for (String line : lines) {

            String[] _cols = StringUtils.splitPreserveAllTokens(line, Utils.TAB);
            UserDO user = UicUtils.getUser(_cols[0]);
            //System.out.println(user);
            //table.addCol(null, _cols[0], _cols[0]);
            table.addCol(null, user.getNick(), user.getNick());
            table.addCol(null, null, NumberUtils.toLong(_cols[1]) > 0 ? "��" : "��");
            table.addCol(_cols[0] + "_trade", "���׶�", Utils.toYuan(_cols[2]));
            table.addCol(null, "���׶�", Fmt.money(NumberUtils.toLong(_cols[2])));
            table.addCol(_cols[0] + "_tradeCount", "���ױ���", _cols[3]);
            table.addCol(null, "��Ӫ��Ŀ", Category.get(_cols[4]));
            table.addCol(Report.newBreakValue());
            if (count++ > maxCount) break;
        }


        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
    }


}