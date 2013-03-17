package org.dueam.hadoop.bp.report;

import org.dueam.hadoop.common.util.MapUtils;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.hadoop.services.Category;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.util.List;
import java.util.Map;

/**
 * 分类目原因统计
 * User: windonly
 * Date: 10-12-20 下午6:08
 */
public class RefundReason {
    static char CTRL_A = (char) 0x01;

    public static void main(String[] args) throws Exception {
        String input = args[0];
        if (!new File(input).exists()) {
            return;
        }

        Map<String, List<String[]>> today = MapUtils.map(Utils.read(input, null), CTRL_A);
        Report report = Report.newReport("各个类目下退款原因统计（最近30天）");
        for (Map.Entry<String, List<String[]>> entry : today.entrySet()) {
            String name = Category.getCategoryName(entry.getKey()) + "类目下退款原因分布";
            Table table1 = report.newGroupTable(entry.getKey() + "_1", name + "(需退货)");
            Table table2 = report.newGroupTable(entry.getKey() + "_0", name + "(无需退货)");
            for (String[] _cols : entry.getValue()) {
                if ("1".equals(_cols[0])) {
                    table1.addCol(_cols[1], REASONS.get(_cols[1]), _cols[2]);
                } else {
                    table2.addCol(_cols[1], REASONS.get(_cols[1]), _cols[2]);
                }
            }
        }


        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
    }


    private static Map<String, String> REASONS = MapUtils.asMap("1,未收到货物,2,卖家缺货,3,拍错商品,4,商品缺少所需样式,5,与卖家协商一致退款,6,卖家未及时发货\t,11,七天无理由退换货,12,商品质量问题,13,收到的商品不符,14,未收到货,15,退还邮费,16,折扣、赠品、发票问题,18,收到假货\t,19,与卖家协商一致退款,20,物流公司发货问题,21,卖家虚假发货,22,卖家没有在24小时内发货,0,其他".split(","));


}