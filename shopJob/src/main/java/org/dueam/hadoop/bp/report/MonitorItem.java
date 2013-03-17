package org.dueam.hadoop.bp.report;

import org.dueam.hadoop.common.util.Fmt;
import org.dueam.hadoop.common.util.MapUtils;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 *  t.item_type,
    count(1) as item_cnt,
    sum(cast(t.if_online as bigint)) as online_cnt,
    sum(t.quantity) as sum_quantity,
    sum(t.ordercost) as sum_ordercost,
    sum(t.member_cnt) as sum_member_cnt,
    sum(t.guest_cnt) as sum_guest_cnt
 * ���˫12��Ʒ
 * User: windonly
 * Date: 11-1-6 ����5:24
 */
public class MonitorItem {
    private static Map<String,String> typeMap = Utils.asLinkedMap("0","�����Ʒ","1","�ۻ�����Ʒ","11","�ۻ�����Ʒ����㿪�ţ�","12","�ۻ�����Ʒ��ʮ�㿪�ţ�","13","�ۻ�����Ʒ��14�㿪�ţ�","2","5�۷���","4","�ۻ��㷿��","3","�����ؼ�","5","��Ŀ��ɱ");

    private static char CTRL_A = (char)0x01;
    public static void main(String[] args) throws IOException {
        org.dueam.report.common.Report report = org.dueam.report.common.Report.newReport("˫12��Ʒ��ر���");
        String input = args[0];
        if (!new File(input).exists()) {
            return;
        }
        Map<String, List<String[]>> today = MapUtils.map(Utils.read(input, null),CTRL_A);
        if (true && today != null) {


            for(String key : typeMap.keySet())  {
                if(today.containsKey(key)) {
                    Table table = report.newTable("table_"+key, typeMap.get(key) + "���ͳ��", "γ��", "����");
                    if(!today.get(key).isEmpty()){
                        String[] cols = today.get(key).get(0);
                        String item_cnt = cols[0], online_cnt = cols[1],sum_quantity = cols[2],sum_ordercost = cols[3],sum_member_cnt = cols[4],sum_guest_cnt=cols[5];
                        String sum_ipv_cnt = cols[6],sum_ipv_uv = cols[7];
                        table.addCol("item_cnt","��Ʒ��",item_cnt);
                        table.addCol("online_cnt","������Ʒ��",online_cnt);
                        table.addCol("sum_quantity","�ܿ��",sum_quantity);
                        table.addCol("avg_quantity","ƽ�����",Fmt.divFixed(sum_quantity,item_cnt));
                        table.addCol("sum_ordercost","���ղ���",sum_ordercost);
                        table.addCol("avg_ordercost","ƽ���ղ���",Fmt.divFixed(sum_ordercost,item_cnt));
                        table.addCol("sum_member_cnt","���빺�ﳵ��������Ա��",sum_member_cnt);
                        table.addCol("avg_member_cnt","ƽ�����빺�ﳵ��������Ա��",Fmt.divFixed(sum_member_cnt,item_cnt));
                        table.addCol("sum_guest_cnt","���빺�ﳵ�������ÿͣ�",sum_guest_cnt);
                        table.addCol("avg_guest_cnt","ƽ�����빺�ﳵ�������ÿͣ�",Fmt.divFixed(sum_guest_cnt,item_cnt));
                        table.addCol("sum_ipv_cnt","IPV",sum_ipv_cnt);
                        table.addCol("avg_ipv_cnt","ƽ��IPV",Fmt.divFixed(sum_ipv_cnt,item_cnt));
                        //table.addCol("sum_ipv_uv","",sum_ipv_uv);
                        table.addCol("avg_ipv_uv","ƽ��IPV-UV",Fmt.divFixed(sum_ipv_uv,item_cnt));
                    }
                }
            }

        }
        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));

    }
}
