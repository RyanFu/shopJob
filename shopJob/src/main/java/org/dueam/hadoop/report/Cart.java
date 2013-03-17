package org.dueam.hadoop.report;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.dueam.hadoop.common.util.Fmt;
import org.dueam.hadoop.common.util.MapUtils;
import org.dueam.hadoop.common.util.Report;
import org.dueam.hadoop.common.util.Utils;

import java.awt.image.ImageFilter;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.dueam.hadoop.common.tables.Cart.status;

/**
 * User: windonly
 * Date: 11-1-6 下午5:24
 */
public class Cart {
    public static void main(String[] args) throws IOException {
        String input = args[0];
        if (!new File(input).exists()) {
            return;
        }

        Map<String, List<String[]>> today = MapUtils.map(Utils.read(input, (String[])null));
        Report.newTable("购物车基本信息", null, null);
        Report.add(new String[]{"纬度", "数量"});
        for (String[] array : today.get("sum")) {
            Report.add(array[0], array[1]);
        }

        for (String[] array : today.get("del")) {
            Report.add(status(array[0]), array[1]);
        }

        if (true && today.containsKey("avg_order")) {
            long sum = 0;
            long count = 0;
            for (String[] array : today.get("avg_order")) {
                if ("sum".equals(array[0])) {
                    sum = NumberUtils.toLong(array[1]);
                } else if ("count".equals(array[0])) {
                    count = NumberUtils.toLong(array[1]);
                }
            }

            Report.add("avg_order","第一次加入购物车到下单的平均时间(单位:S)", Fmt.divNumber(sum, count));
        }

        if (true && today.containsKey("avg_last")) {
            long sum = 0;
            long count = 0;
            for (String[] array : today.get("avg_last")) {
                if ("sum".equals(array[0])) {
                    sum = NumberUtils.toLong(array[1]);
                } else if ("count".equals(array[0])) {
                    count = NumberUtils.toLong(array[1]);
                }
            }
            Report.add("avg_last","最后次加入购物车到下单的平均时间(单位:S)", Fmt.divNumber(sum, count));
        }

        if (true) {
            Report.newTable("商品第一次加入购物车到下单的时间分布(GMT_MODIFIED - GMT_CREATE)");
            Report.add("区间", "数量", "占比");
            Map<String, String> map = new HashMap<String, String>();
            long sum = 0;
            for (String[] array : today.get("order")) {
                map.put(array[0], array[1]);
                sum += NumberUtils.toLong(array[1], 0);
            }
            for (String key : org.dueam.hadoop.conf.Cart.areaKeys) {
                Report.add(key, map.get(key), Fmt.parent(NumberUtils.toLong(map.get(key)), sum));
            }
        }

        if (true && today.get("last") != null && !today.get("last").isEmpty()) {
            Report.newTable("商品最近一次加入购物车到下单的时间分布(GMT_MODIFIED - LAST_MODIFIED)");
            Report.add("区间", "数量", "占比");
            Map<String, String> map = new HashMap<String, String>();
            long sum = 0;
            for (String[] array : today.get("last")) {
                map.put(array[0], array[1]);
                sum += NumberUtils.toLong(array[1], 0);
            }
            for (String key : org.dueam.hadoop.conf.Cart.areaKeys) {
                Report.add(key, map.get(key), Fmt.parent(NumberUtils.toLong(map.get(key)), sum));
            }
        }

        FileUtils.writeStringToFile(new File(args[0] + ".html"), Report.renderHtml(null), "GBK");

    }
}
