package org.dueam.hadoop.bp.report;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.dueam.hadoop.common.LineFile;
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
import java.util.*;

import static org.apache.commons.lang.math.NumberUtils.toInt;
import static org.dueam.hadoop.common.Functions.newHashMap;
import static org.dueam.hadoop.common.Functions.str;

/**
 * C2CŮװ��Ʒ����
 */
public class NzNewItem {
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

        Report report = Report.newReport("C2CŮװ��Ʒ����");
        int row = 13;
        /**
         cast(sum(if(a1.is_xijie>0,cast(b1.ali_fee as double),0)) as bigint),
         sum(if(a1.is_xijie>0,cast(b1.ali_cnt as bigint),0)),
         sum(if(a1.is_xijie>0,cast(b1.ali_uv as bigint),0)),
         sum(if(a1.is_xijie>0,cast(c1.ipv_cnt as bigint),0)),
         sum(if(a1.is_xijie>0,cast(c1.c_search_jump_ipv_cnt as bigint),0)),
         sum(if(a1.is_xijie>0,cast(c1.list_jump_ipv_cnt as bigint),0)),
         sum(if(a1.is_xijie>0,cast(c1.shop_jump_ipv_cnt as bigint),0)),
         sum(if(a1.is_xijie>0,cast(c1.ipv_uv as bigint),0)),
         sum(if(a1.is_xijie>0,cast(c1.c_search_jump_ipv_uv as bigint),0)),
         sum(if(a1.is_xijie>0,cast(c1.list_jump_ipv_uv as bigint),0)),
         sum(if(a1.is_xijie>0,cast(c1.shop_jump_ipv_uv as bigint),0)),
         sum(if(a1.is_xijie>0,cast(a1.ordercost as bigint),0)),
         sum(if(a1.is_xijie>0,1,0)),
         */
        Table totalMonth = report.newViewTable("total_month", "ȫ����Ʒ����ȥ30�췢������������");
        Table xjMonth = report.newViewTable("xj_month", "ϸ��ʵ����Ʒ����ȥ30�췢������������");
        Table totalToday = report.newViewTable("total_today", "ȫ����Ʒ�����췢���ı���������");
        Table xjToday = report.newViewTable("xj_today", "ϸ��ʵ����Ʒ�����췢���ı���������");
        Table totalXj = report.newViewTable("today_xj_month", "ȫ��ϸ��ʵ�ı�������");
        Table[] tables = new Table[]{totalXj, xjToday, xjMonth, totalToday, totalMonth};
        for (Table table : tables) {
            table.addCol("��Ŀ����").addCol("IPV")//.addCol("IPV_UV").addCol("����ת����")
                    .addCol("���ת����").addCol("��Ʒ��").addCol("���׶�").addCol("�͵���")
                    .addCol("�ʵ���").addCol("�ղ���").breakRow();
        }
        List<String> lines = Utils.read(args[0]);
        Collections.sort(lines, new Comparator<String>() {
            public int compare(String o1, String o2) {
                String[] _cols1 = StringUtils.splitPreserveAllTokens(o1, CTRL_A);
                String[] _cols2 = StringUtils.splitPreserveAllTokens(o2, CTRL_A);
                return NumberUtils.toLong(_cols1[4]) > NumberUtils.toLong(_cols2[4]) ? -1 : 1;
            }
        });
        for (String line : lines) {
            String[] _cols = StringUtils.splitPreserveAllTokens(line, CTRL_A);
            String catId = _cols[0];
            for (int i = 1; i < _cols.length; i += row) {
                String[] _array = Utils.subArray(_cols, i, i + row);
                System.out.println(i + "\t" + (i / row) + "\t" + _array.length + "\t" + Arrays.asList(_array));
                Table table = tables[i / row];
                String ali_fee = _array[0];
                String ali_cnt = _array[1];
                String ali_uv = _array[2];
                String ipv_cnt = _array[3];
                String c_search_jump_ipv_cnt = _array[4];
                String list_jump_ipv_cnt = _array[5];
                String shop_jump_ipv_cnt = _array[6];
                String ipv_uv = _array[7];
                String c_search_jump_ipv_uv = _array[8];
                String list_jump_ipv_uv = _array[9];
                String shop_jump_ipv_uv = _array[10];
                String fav_cnt = _array[11];
                String item_cnt = _array[12];
                table.addCol(Category.getCategoryName(catId)).addCol(ipv_cnt)//.addCol(Fmt.moneyFmt(ipv_uv)).addCol(Fmt.parent2(ali_uv, ipv_uv))
                        .addCol(Fmt.parent2(ali_cnt, ipv_cnt)).addCol((item_cnt)).addCol((ali_fee)).addCol(Fmt.div(ali_fee, ali_uv))
                        .addCol(Fmt.div(ali_fee, ali_cnt)).addCol((fav_cnt)).breakRow();
            }

        }


        for (Table table : tables) {
            table.addCol("�ϼ�").addCol(str((int)table.sumRow(1)))//.addCol("IPV_UV").addCol("����ת����")
                    .addCol("-").addCol(str((int)table.sumRow(3))).addCol(str((int)table.sumRow(4))).addCol("-")
                    .addCol("-").addCol(str((int)table.sumRow(7))).breakRow();
        }

        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
    }


}
