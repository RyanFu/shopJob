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
 * 监控双12商品
 * User: windonly
 * Date: 11-1-6 下午5:24
 */
public class MonitorItem {
    private static Map<String,String> typeMap = Utils.asLinkedMap("0","大促商品","1","聚划算商品","11","聚划算商品（零点开团）","12","聚划算商品（十点开团）","13","聚划算商品（14点开团）","2","5折房产","4","聚划算房产","3","天天特价","5","类目秒杀");

    private static char CTRL_A = (char)0x01;
    public static void main(String[] args) throws IOException {
        org.dueam.report.common.Report report = org.dueam.report.common.Report.newReport("双12商品监控报表");
        String input = args[0];
        if (!new File(input).exists()) {
            return;
        }
        Map<String, List<String[]>> today = MapUtils.map(Utils.read(input, null),CTRL_A);
        if (true && today != null) {


            for(String key : typeMap.keySet())  {
                if(today.containsKey(key)) {
                    Table table = report.newTable("table_"+key, typeMap.get(key) + "相关统计", "纬度", "数量");
                    if(!today.get(key).isEmpty()){
                        String[] cols = today.get(key).get(0);
                        String item_cnt = cols[0], online_cnt = cols[1],sum_quantity = cols[2],sum_ordercost = cols[3],sum_member_cnt = cols[4],sum_guest_cnt=cols[5];
                        String sum_ipv_cnt = cols[6],sum_ipv_uv = cols[7];
                        table.addCol("item_cnt","商品数",item_cnt);
                        table.addCol("online_cnt","在线商品数",online_cnt);
                        table.addCol("sum_quantity","总库存",sum_quantity);
                        table.addCol("avg_quantity","平均库存",Fmt.divFixed(sum_quantity,item_cnt));
                        table.addCol("sum_ordercost","总收藏量",sum_ordercost);
                        table.addCol("avg_ordercost","平均收藏量",Fmt.divFixed(sum_ordercost,item_cnt));
                        table.addCol("sum_member_cnt","加入购物车总数（会员）",sum_member_cnt);
                        table.addCol("avg_member_cnt","平均加入购物车总数（会员）",Fmt.divFixed(sum_member_cnt,item_cnt));
                        table.addCol("sum_guest_cnt","加入购物车总数（访客）",sum_guest_cnt);
                        table.addCol("avg_guest_cnt","平均加入购物车总数（访客）",Fmt.divFixed(sum_guest_cnt,item_cnt));
                        table.addCol("sum_ipv_cnt","IPV",sum_ipv_cnt);
                        table.addCol("avg_ipv_cnt","平均IPV",Fmt.divFixed(sum_ipv_cnt,item_cnt));
                        //table.addCol("sum_ipv_uv","",sum_ipv_uv);
                        table.addCol("avg_ipv_uv","平均IPV-UV",Fmt.divFixed(sum_ipv_uv,item_cnt));
                    }
                }
            }

        }
        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));

    }
}
