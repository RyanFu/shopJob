package org.dueam.hadoop.bp.report;

import org.apache.commons.lang.math.NumberUtils;
import org.dueam.hadoop.common.util.ItemUtils;
import org.dueam.hadoop.common.util.MapUtils;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * User: windonly
 * Date: 11-1-6 下午5:24
 */
public class Auctions {
    public static void main(String[] args) throws IOException {
        String input = args[0];
        if (!new File(input).exists()) {
            return;
        }
        Report report = Report.newReport("拍卖宝贝追踪报表");
        Map<String, List<String[]>> today = MapUtils.map(Utils.read(input, null));
        if (today.get("total") != null) {
            Table table = report.newTable("total","全网拍卖宝贝基本信息");
            for (String[] array : today.get("total")) {
                if (NumberUtils.isNumber(array[0])) {
                    table.addCol(array[0], ItemUtils.getItemStatusName(array[0]), array[1]);
                } else {
                    table.addCol(array[0], getName(array[0]), array[1]);
                }
            }
            today.remove("total");
        }
        for (Map.Entry<String, List<String[]>> entry : today.entrySet()) {
            Table table = report.newTable("today_"+entry.getKey(),getName(entry.getKey())+"拍卖宝贝信息");
            for (String[] array : entry.getValue()) {
                table.addCol(array[0],getName(array[0]), array[1]);
            }

        }
        XmlReportFactory.dump(report,new FileOutputStream(args[0] + ".xml"));

    }

    public static String getName(String key) {
        String value = namesMap.get(key);
        if (value != null) return value;
        return key;
    }

    static Map<String, String> namesMap = Utils.asMap("all,全部,many,荷兰拍,one,单件拍,publish,今天发布,start,今天上架,ends,今天下架,ends_unbid,今天下架（未出价）,ends_bid,今天下架（已出价）,bid,已经出价,unbid,从未出价".split(","));
}
