package org.dueam.hadoop.bp.report;

import org.apache.commons.lang.math.NumberUtils;
import org.dueam.hadoop.common.KeyValuePair;
import org.dueam.hadoop.common.MergeUtils;
import org.dueam.hadoop.common.util.MapUtils;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.hadoop.services.Category;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.Value;
import org.dueam.report.common.XmlReportFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.dueam.hadoop.common.Functions.*;

/**
 * User: windonly
 * Date: 11-1-6 ����5:24
 */
public class TradeAuctionPriceStat {
    private static MergeUtils.KeyMerger KEY_MERGER =
            new MergeUtils.KeyMerger() {
                public String merge(KeyValuePair
                                            o1, KeyValuePair
                        o2, boolean isLast) {
                    if (isLast) return NumberUtils.toInt(o1.getKey()) * 10 + "-max";
                    StringBuffer nameBuffer = new StringBuffer();
                    nameBuffer.append(NumberUtils.toInt(o1.getKey()) * 10).append("-");
                    if (o2 == null) {
                        nameBuffer.append(NumberUtils.toInt(o1.getKey()) * 10 + 9);
                    } else {
                        nameBuffer.append(NumberUtils.toInt(o2.getKey()) * 10 + 9);
                    }
                    return nameBuffer.toString();
                }
            };

    public static void main(String[] args) throws IOException {
        Report report = Report.newReport("C2CŮװ��50�콻�ױ����۸�ֲ�");
        String input = args[0];
        if (!new File(input).exists()) {
            return;
        }
        Map<String, List<String[]>> today = MapUtils.map(Utils.read(input, null));
        for(Map.Entry<String,List<String[]>> entry : today.entrySet()){
            Table table = report.newGroupTable("stat_" + entry.getKey(), Category.get(entry.getKey()) + "�ѷ������ױ����۸�ֲ�", "�۸�����(Ԫ)", "������");
            Table table2 = report.newGroupTable("stat_trade_" + entry.getKey(), Category.get(entry.getKey()) + "�����۸�����Ľ��ֲ�", "�۸�����(Ԫ)", "���(Ԫ)");
            List<KeyValuePair> pairs = newArrayList();
            for(String[] array : entry.getValue()){
                 pairs.add(KeyValuePair.newPair(array[0], toLong(array[1]),toLong(array[2])));
            }
            long mergeValue = MergeUtils.sum(pairs) / 10;
            boolean isFirst = true;
            for(KeyValuePair pair : MergeUtils.merge(pairs,mergeValue,KEY_MERGER)){
                Value value = Report.newValue(pair.getKey(),str(pair.getValue()));
                table.addCol(value);
                if(isFirst){
                    isFirst =false;
                    int count = 0;
                    for(KeyValuePair _pair : pairs){
                        if(count ++ > 30) break;
                        value.addDetail(KEY_MERGER.merge(_pair, null, false), str(_pair.getValue()));
                    }
                }
                table2.addCol(Report.newValue(pair.getKey(),Utils.toYuan(str(pair.getExtValue()))));
            }

        }

        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"), args[0], null);

    }
}
