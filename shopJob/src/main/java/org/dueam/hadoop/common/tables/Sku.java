package org.dueam.hadoop.common.tables;

import java.util.ArrayList;
import java.util.List;

/**
 * User: windonly
 * Date: 11-4-7 ÉÏÎç10:01
 */
public class Sku {
    public final static int sku_id = 0;
    public final static int item_id = 1;
    public final static int properties = 2;
    public final static int quantity = 3;
    public final static int price = 4;
    public final static int status = 5;
    public final static int outer_id = 6;
    public final static int gmt_create = 7;
    public final static int gmt_modified = 8;
    public final static int seller_id = 9;
    public final static int sync_version = 10;
    public final static int vertical_market = 11;

    public static long[] area = new long[]{1,2,5,10,20,30,40,50,60,70,80,90,100,120,140,160,180,200,500,1000,2000};

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

    public static void main(String[] args){
        System.out.println(getArea(100));
        System.out.println(areaKeys);
    }
}
