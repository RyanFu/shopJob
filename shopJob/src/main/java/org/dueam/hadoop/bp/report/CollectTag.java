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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.dueam.hadoop.common.Functions.*;

/**
 * User: windonly
 * Date: 11-1-6 下午5:24
 */
public class CollectTag {

    public static void main(String[] args) throws IOException {
        Report report = Report.newReport("女装类目用户收藏Tag统计");
        args = new String[]{"20110811"};
        String input = args[0];
        if (!new File(input).exists()) {
            return;
        }
        Map<String, List<String[]>> today = MapUtils.map(Utils.readWithCharset(input, "utf-8"));
        for (String key : new String[]{"1", "0"}) {
            String itemType = ("1".equals(key) ? "宝贝" : "店铺");
            Table table = report.newTable("stat_" + key, itemType + "收藏标签排行", "收藏标签", "收藏数");
            List<KeyValuePair> pairs = newArrayList();
            for (String[] array : today.get(key)) {
                pairs.add(KeyValuePair.newPair(array[0], toLong(array[1]), KeyValuePair.NumberValueDesc));
            }
            Collections.sort(pairs);
            int count = 0;
            for (KeyValuePair pair : pairs) {
                if (count++ > 1000) break;
                Value value = Report.newValue(pair.getKey(), str(pair.getValue()));
                table.addCol(value);
            }

        }

        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"), args[0], null);

    }
}
