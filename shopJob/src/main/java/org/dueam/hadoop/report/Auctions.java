package org.dueam.hadoop.report;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.dueam.hadoop.common.util.*;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.dueam.hadoop.common.tables.Cart.status;

/**
 * User: windonly
 * Date: 11-1-6 ����5:24
 */
public class Auctions {
    public static void main(String[] args) throws IOException {
        String input = args[0];
        if (!new File(input).exists()) {
            return;
        }

        Map<String, List<String[]>> today = MapUtils.map(Utils.read(input, null));
        if (today.get("total") != null) {
            Report.newTable("ȫ����������������Ϣ", null, null);
            Report.add(new String[]{"γ��", "����"});
            for (String[] array : today.get("total")) {
                if (NumberUtils.isNumber(array[0])) {
                    Report.add(ItemUtils.getItemStatusName(array[0]), array[1]);
                } else {
                    Report.add(getName(array[0]), array[1]);
                }
            }
            today.remove("total");
        }
        for (Map.Entry<String, List<String[]>> entry : today.entrySet()) {
            Report.newTable(getName(entry.getKey()) + "����������Ϣ", null, null);
            Report.add(new String[]{"γ��", "����"});
            for (String[] array : entry.getValue()) {
                Report.add(getName(array[0]), array[1]);
            }

        }

        FileUtils.writeStringToFile(new File(args[0] + ".html"), Report.renderHtml(null), "GBK");

    }

    public static String getName(String key) {
        String value = namesMap.get(key);
        if (value != null) return value;
        return key;
    }

    static Map<String, String> namesMap = Utils.asMap("all,ȫ��,many,������,one,������,publish,���췢��,start,�����ϼ�,ends,�����¼�,ends_unbid,�����¼ܣ�δ���ۣ�,ends_bid,�����¼ܣ��ѳ��ۣ�,bid,�Ѿ�����,unbid,��δ����".split(","));
}
