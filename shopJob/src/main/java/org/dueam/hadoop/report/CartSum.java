package org.dueam.hadoop.report;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.dueam.hadoop.common.util.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.dueam.hadoop.common.tables.Cart.status;

/**
 * User: windonly
 * Date: 11-1-6 下午5:24
 */
public class CartSum {
    public static void main(String[] args) throws IOException {
        String input = args[0];
        if (!new File(input).exists()) {
            return;
        }
        List<String> cartBase = new ArrayList<String>();
        List<String> cartGmt = new ArrayList<String>();
        List<String> cartLast = new ArrayList<String>();
        StringBuffer headerBase = new StringBuffer("时间");
        StringBuffer headerGmt = new StringBuffer("时间");
        StringBuffer headerLast = new StringBuffer("时间");
        for (int i = 0; ; i++) {
            String time = DateStringUtils.add(input, -i);
            if (!new File(time).exists()) {
                break;
            }
            Map<String, List<String[]>> today = MapUtils.map(Utils.read(time, null));
            StringBuffer baseBuffer = new StringBuffer();


            StringBuffer base = new StringBuffer(time);
            StringBuffer gmt = new StringBuffer(time);
            StringBuffer last = new StringBuffer(time);
            for (String[] array : today.get("sum")) {
                if (i == 0) {
                    headerBase.append(',').append(array[0]);
                }
                base.append(',').append(array[1]);
            }

            for (String[] array : today.get("del")) {
                if (i == 0) {
                    headerBase.append(',').append(status(array[0]));
                }
                base.append(',').append(array[1]);
            }
            if (true) {
                Map<String, String> map = new HashMap<String, String>();
                long sum = 0;
                for (String[] array : today.get("order")) {
                    map.put(array[0], array[1]);
                    sum += NumberUtils.toLong(array[1], 0);
                }

                for (String key : org.dueam.hadoop.conf.Cart.areaKeys) {
                    if (i == 0) {
                        headerGmt.append(',').append(key);
                    }
                    gmt.append(',').append(map.get(key));
                }
            }

            if (true && today.get("last") != null && !today.get("last").isEmpty()) {
                Map<String, String> map = new HashMap<String, String>();
                long sum = 0;
                for (String[] array : today.get("last")) {
                    map.put(array[0], array[1]);
                    sum += NumberUtils.toLong(array[1], 0);
                }

                for (String key : org.dueam.hadoop.conf.Cart.areaKeys) {
                    if (i == 0) {
                        headerLast.append(',').append(key);
                    }
                    last.append(',').append(map.get(key));
                }
            }
            if (i == 0) {
                cartBase.add(headerBase.toString());
                cartGmt.add(headerGmt.toString());
                cartLast.add(headerLast.toString());
            }
            cartBase.add(base.toString());
            cartGmt.add(gmt.toString());
            cartLast.add(last.toString());
        }

        FileUtils.writeLines(new File("购物车基本信息.csv"), "gbk", cartBase);
        FileUtils.writeLines(new File("商品第一次加入购物车到下单的时间分布(GMT_MODIFIED - GMT_CREATE).csv"), "gbk", cartGmt);
        FileUtils.writeLines(new File("商品最近一次加入购物车到下单的时间分布(GMT_MODIFIED - LAST_MODIFIED).csv"), "gbk", cartLast);


    }
}
