package org.dueam.hadoop.bp.report;

import static org.dueam.hadoop.common.Functions.str;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.dueam.hadoop.common.util.Fmt;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

/**
 * ��Ʒͳ��
 */
public class Xinpin {
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

        Report report = Report.newReport("��Ʒҵ�񱨱�");

        Table xp = report.newViewTable("today_xj_month", "��Ʒ����ͳ��");

        xp.addCol("��Ŀ����").addCol("IPV")//.addCol("IPV_UV").addCol("����ת����")
                .addCol("���ת����").addCol("��Ʒ��").addCol("���׶�").addCol("�͵���").addCol("�ʵ���").addCol("�ղ���").breakRow();

        Table online = report.newTable("online", "��Ʒ������Ʒ�ֲ�");
        Table offline = report.newTable("offline", "��Ʒ��������Ʒ�ֲ�");

        for (String line : Utils.readWithCharset(input, "utf-8")) {
            String[] _allCols = StringUtils.splitPreserveAllTokens(line, CTRL_A);

            String type = _allCols[0];
            String status = _allCols[2];

            if ("cat".equalsIgnoreCase(type)) {
                if ("1".equalsIgnoreCase(status)) {
                    online.addCol(_allCols[1], _allCols[3]);
                } else if ("0".equalsIgnoreCase(status)) {
                    offline.addCol(_allCols[1], _allCols[3]);
                }
            }

            if ("cat2".equalsIgnoreCase(type)) {

                String cat = _allCols[1];
                String ali_fee = _allCols[2];
                String ali_cnt = _allCols[3];
                String ali_uv = _allCols[4];
                String ipv_cnt = _allCols[5];
                String c_search_jump_ipv_cnt = _allCols[6];
                String list_jump_ipv_cnt = _allCols[7];
                String shop_jump_ipv_cnt = _allCols[8];
                String ipv_uv = _allCols[9];
                String c_search_jump_ipv_uv = _allCols[10];
                String list_jump_ipv_uv = _allCols[11];
                String shop_jump_ipv_uv = _allCols[12];
                String fav_cnt = _allCols[13];
                String item_cnt = _allCols[14];
                xp.addCol(cat).addCol(ipv_cnt)
                        //.addCol(Fmt.moneyFmt(ipv_uv)).addCol(Fmt.parent2(ali_uv, ipv_uv))
                        .addCol(Fmt.parent2(ali_cnt, ipv_cnt)).addCol((item_cnt)).addCol((ali_fee)).addCol(
                                Fmt.div(ali_fee, ali_uv)).addCol(Fmt.div(ali_fee, ali_cnt)).addCol((fav_cnt))
                        .breakRow();
            }
        }

        online.addCol("�ϼ�", str(online.getSum()));

        offline.addCol("�ϼ�", str(offline.getSum()));

        xp.addCol("�ϼ�").addCol(str((int) xp.sumRow(1)))
                //.addCol("IPV_UV").addCol("����ת����")
                .addCol("-").addCol(str((int) xp.sumRow(3))).addCol(str((int) xp.sumRow(4))).addCol("-").addCol("-")
                .addCol(str((int) xp.sumRow(7))).breakRow();

        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
    }
}
