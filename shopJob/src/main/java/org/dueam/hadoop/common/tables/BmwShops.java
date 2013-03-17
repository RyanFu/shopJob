package org.dueam.hadoop.common.tables;

import java.util.ArrayList;
import java.util.List;

/**
 * User: windonly
 * Date: 11-4-12 ÏÂÎç3:59
 */
public class BmwShops {
    public final static int shop_id = 0;
    public final static int title = 1;
    public final static int starts = 2;
    public final static int pict_url = 3;
    public final static int category = 4;
    public final static int approve_status = 5;
    public final static int promoted_status = 6;
    public final static int nick = 7;
    public final static int products_count = 8;
    public final static int products_count_time = 9;
    public final static int theme = 10;
    public final static int key_biz = 11;
    public final static int manage_state = 12;
    public final static int manage_time = 13;
    public final static int manage_user = 14;
    public final static int gmt_modified = 15;
    public final static int collector_count = 16;
    public final static int tfs_path = 17;
    public final static int user_id = 18;
    public final static int feature = 19;
    public final static int superShopLogo = 20;
    public final static int siteid = 21;
    public final static int feature_cc = 22;
    public final static int wap_feature = 23;

    public static long[] area = new long[]{0,1,10,25,50,100,200,500,1000,2000,5000,10000};

    public static List<String> areaKeys = new ArrayList<String>();

    static {
        for (long _value : area) {
            areaKeys.add(getArea(_value));
        }
    }

    public static String getArea(long value) {
        int pos = 0;
        for (long area_value : area) {
            if (value < area_value)
                return "[" + area[pos - 1] + "-" + (area[pos] - 1) + "]";
            pos++;
        }
        return "[" + area[area.length - 1] + "-max]";
    }
}
