package org.dueam.hadoop.common;

import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.dom4j.datatype.DatatypeElement;

import java.io.SequenceInputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * a table in base on hadoop
 * User: windonly
 * Date: 10-12-16 ÏÂÎç4:29
 */
public class HadoopTable implements Table {
    private String name;//table name
    private Class inputFormat;//input format
    private String[] inputPath; //input path

    public String getName() {
        return name;
    }

    public Class getInputFormat() {
        return inputFormat;
    }

    public String[] getInputPath() {
        return inputPath;
    }

    public Table newInstance(String date) {
        String[] inputPath = this.callback.getInputPath(date);
        return new HadoopTable(this.name, this.inputFormat, inputPath);
    }

    public HadoopTable(String name, Class inputFormat, String[] inputPath) {
        this.name = name;
        this.inputFormat = inputFormat;
        this.inputPath = inputPath;
    }

    public static interface InputPathCallback {
        public String[] getInputPath(String date);
    }

    InputPathCallback callback;

    public HadoopTable(String name, Class inputFormat, InputPathCallback callback) {
        this.name = name;
        this.inputFormat = inputFormat;
        this.callback = callback;
    }

    public HadoopTable(String name, Class inputFormat, InputPathCallback callback, String date) {
        this.name = name;
        this.inputFormat = inputFormat;
        this.callback = callback;
        if (date != null) {
            this.inputPath = callback.getInputPath(date);
        }
    }

    private static Map<String, Table> cache = new HashMap<String, Table>();

    private static void register(Table table, String... alias) {
        for (String key : alias) {
            cache.put(key, table);
        }
    }

    static {
        register(auction("20101010"), "auction");
        register(order("20101010"), "order");
    }

    public static Table auction(String date) {
        return new HadoopTable("auction_auctions", TextInputFormat.class, new InputPathCallback() {
            public String[] getInputPath(String date) {
                return new String[]{hive("s_auction_auctions", date)};
            }
        }, date);
    }

    public static Table refund(String date) {
        return new HadoopTable("tc_refund_trade", TextInputFormat.class, new InputPathCallback() {
            public String[] getInputPath(String date) {
                return new String[]{stb("tc_refund_trade", date)};
            }
        }, date);
    }

    public static Table auctionR(String date) {
        return new HadoopTable("auction_auctions", SequenceFileInputFormat.class, new InputPathCallback() {
            public String[] getInputPath(String date) {
                return new String[]{hive("r_auction_auctions", date) + "/if_online=0", hive("r_auction_auctions", date) + "/if_online=1"};
            }
        }, date);
    }

    public static Table auctionOnline(String date) {
        return new HadoopTable("auction_auctions", SequenceFileInputFormat.class, new InputPathCallback() {
            public String[] getInputPath(String date) {
                return new String[]{hive("r_auction_auctions", date) + "/if_online=1"};
            }
        }, date);
    }

    public static Table order(String date) {
        return new HadoopTable("tc_biz_order", SequenceFileInputFormat.class, new InputPathCallback() {
            public String[] getInputPath(String date) {
                return new String[]{stb("tc_biz_order", date)};
            }
        }, date);
    }

    public static Table order(String date, final String days) {
        return new HadoopTable("tc_biz_order", SequenceFileInputFormat.class, new InputPathCallback() {
            public String[] getInputPath(String date) {
                return new String[]{stb("tc_biz_order", date, days)};
            }
        }, date);
    }


    public static Table collectInfoAll(String date) {
        return new HadoopTable("collect_info_all", TextInputFormat.class, new InputPathCallback() {
            public String[] getInputPath(String date) {
                return new String[]{stb("collect_info_all", date)};
            }
        }, date);
    }
    public static Table feedReceive(String date, final String days) {
        return new HadoopTable("feed_receive", TextInputFormat.class, new InputPathCallback() {
            public String[] getInputPath(String date) {
                return new String[]{stb("feed_receive", date, days)};
            }
        }, date);
    }


    public static Table cmCategory(String date) {
        return new HadoopTable("cm_categories", TextInputFormat.class, new InputPathCallback() {
            public String[] getInputPath(String date) {
                return new String[]{stb("cm_categories", date)};
            }
        }, date);
    }

    public static Table shop(String date) {
        return new HadoopTable("bmw_shops", TextInputFormat.class, new InputPathCallback() {
            public String[] getInputPath(String date) {
                return new String[]{stb("bmw_shops", date)};
            }
        }, date);
    }

    public static Table sku(String date) {
        return new HadoopTable("sku", TextInputFormat.class, new InputPathCallback() {
            public String[] getInputPath(String date) {
                return new String[]{stb("sku", date)};
            }
        }, date);
    }

    public static Table dbSku(String date) {
        return new HadoopTable("dbSku", TextInputFormat.class, new InputPathCallback() {
            public String[] getInputPath(String date) {
                return new String[]{"/group/tbptd-dev/xiaodu/hive/db_sku/" + "/pt=" + date + "000000/"};
            }
        }, date);
    }

    public static Table orderDelta(String date) {
        return new HadoopTable("tc_biz_order", TextInputFormat.class, new InputPathCallback() {
            public String[] getInputPath(String date) {
                return new String[]{stb("tc_biz_order_delta", date)};
            }
        }, date);
    }

    public static Table bmwUsersDelta(String date) {
        return new HadoopTable("bmw_users", TextInputFormat.class, new InputPathCallback() {
            public String[] getInputPath(String date) {
                return new String[]{stb("bmw_users_delta", date)};
            }
        }, date);
    }

    public static Table bmwUsers(String date) {
        return new HadoopTable("bmw_users", TextInputFormat.class, new InputPathCallback() {
            public String[] getInputPath(String date) {
                return new String[]{hive("s_bmw_users", date)};
            }
        }, date);
    }

    public static Table table(String name, String date) {
        return cache.get(name).newInstance(date);
    }

    /**
     * hive tables
     *
     * @param table
     * @param date
     * @return
     */
    private static String hive(String table, String date) {
        if (date == null || date.length() != 8 || table == null) {
            throw new IllegalArgumentException(
                    "Please enter table and date , eg. auction_auctions,20100508");
        }
        return "/group/taobao/taobao/hive/" + table + "/pt=" + date + "000000/";
    }

    /**
     * stb tables
     *
     * @param table
     * @param date
     * @return
     */
    private static String stb(String table, String date) {
        if (date == null || date.length() != 8 || table == null) {
            throw new IllegalArgumentException(
                    "Please enter table and date , eg. auction_auctions , 20100508 as curr args : " + table + " - " + date);
        }
        return "/group/taobao/taobao/dw/stb/" + date + "/" + table + "/";
    }

    private static String stb(String table, String date, String days) {
        if (date == null || date.length() != 8 || table == null) {
            throw new IllegalArgumentException(
                    "Please enter table and date , eg. auction_auctions , 20100508 as curr args : " + table + " - " + date);
        }
        return "/group/taobao/taobao/dw/stb/" + date + "/" + table + "/days=" + days;
    }

    public static void main(String[] args) {
        System.out.println(order("20101010").getInputPath());
        System.out.println(atpanel("20110101"));
    }

    public static String atpanel(String date) {
        return "/group/taobao/taobao/dw/log1/" + date.substring(0, 4) + "/" + date;
    }

    public static String cart(String date) {
        return stb("cart", date);
    }


    public static Class<? extends InputFormat> inputFormat(String type) {
        if ("1".equals(type) || "SequenceFileInputFormat".equals(type)) {
            return SequenceFileInputFormat.class;
        }
        return TextInputFormat.class;
    }

    public static String suoniOutput(String name,String date){
       return "/group/tbdev/xiaodu/suoni/" + name + "/"+ date;
    }
}
