package org.dueam.hadoop.bp.report;

import org.apache.commons.lang.math.NumberUtils;
import org.dueam.hadoop.common.util.MapUtils;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.hadoop.utils.uic.UicUtils;
import org.dueam.hadoop.utils.uic.UserDO;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * 交易监控报表
 */
public class TradeTopMonitor {
    static Map<String, String> keyMap = Utils.asLinkedMap(
            "created",
            "创建记录数",
            "modified",
            "修改记录数",
            "gmv",
            "GMV",
            "gmv_num",
            "GMV笔数",
            "alipay",
            "支付宝交易额",
            "alipay_num",
            "支付宝笔数");

    static Map<String, String> typeMap = Utils.asLinkedMap(
            "all",
            "全网",
            "b2c",
            "淘宝商城",
            "c2c",
            "淘宝集市（C2C）",
            "jhs",
            "聚划算");
    private final static int TOP_MAX = 100;

    /**
     * @param args
     * @throws java.io.IOException
     */
    public static void main(String[] args) throws IOException {
        Report report = Report.newReport("交易TOP监控报表");
        String input = args[0];
        if (!new File(input).exists()) {
            System.out.println("File Not Exist ! => " + input);
            return;
        }
        if (true) {
            Map<String, List<String[]>> statusMap = MapUtils.map(Utils.readInclude(input, new String[]{"small_order"}));
            if (statusMap.containsKey("small_order")) {
                Table table = report.newTable("small_order", "小额订单卖家交易笔数TOP 100");
                List<String[]> list = statusMap.get("small_order");
                Collections.sort(list, new Comparator<String[]>() {
                    public int compare(String[] o1, String[] o2) {
                        return NumberUtils.toLong(o1[1]) < NumberUtils.toLong(o2[1]) ? 1 : -1;
                    }
                });
                int count = 0;
                for (String[] array : list) {
                    if (null != array && array.length > 1) {
                        UserDO user = UicUtils.getUser(array[0]);
                        table.addCol(array[0],user.getNick() + " - [" + user.getSellerRateMessage() + "]", Utils.toYuan(array[1]));
                    }
                    if (count++ > TOP_MAX) break;
                }
            }
        }


        //seller top 20
        if (true) {
            Map<String, List<String[]>> today = MapUtils.map(Utils.readInclude(input, new String[]{"seller"}));
            if (today.containsKey("seller")) {
                Map<String, List<String[]>> statusMap = MapUtils.map(today.get("seller"));
                for (String type : new String[]{"all", "b2c", "c2c", "jhs"}) {
                    if (true) {
                        Table table = report.newTable("seller_top_" + type, typeMap.get(type) + "卖家交易金额TOP 100");
                        List<String[]> list = statusMap.get(type);
                        Collections.sort(list, new Comparator<String[]>() {
                            public int compare(String[] o1, String[] o2) {
                                return NumberUtils.toLong(o1[1]) < NumberUtils.toLong(o2[1]) ? 1 : -1;
                            }
                        });
                        int count = 0;
                        for (String[] array : list) {
                            if (null != array && array.length > 1) {
                                UserDO user = UicUtils.getUser(array[0]);
                                table.addCol(array[0],user.getNick() + " - [" + user.getSellerRateMessage() + "]", Utils.toYuan(array[1]));
                            }
                            if (count++ > TOP_MAX) break;
                        }
                    }
                }

            }
        }

        //seller top 20
        if (true) {
            Map<String, List<String[]>> today = MapUtils.map(Utils.readInclude(input, new String[]{"buyer"}));
            if (today.containsKey("buyer")) {
                Map<String, List<String[]>> statusMap = MapUtils.map(today.get("buyer"));
                for (String type : new String[]{"all"}) {
                    if (true) {
                        Table table = report.newTable("buyer_top_" + type, typeMap.get(type) + "买家交易金额TOP 100");
                        List<String[]> list = statusMap.get(type);
                        Collections.sort(list, new Comparator<String[]>() {
                            public int compare(String[] o1, String[] o2) {
                                return NumberUtils.toLong(o1[1]) < NumberUtils.toLong(o2[1]) ? 1 : -1;
                            }
                        });
                        int count = 0;
                        for (String[] array : list) {
                            if (null != array && array.length > 1) {
                                UserDO user = UicUtils.getUser(array[0]);
                                table.addCol(array[0],user.getNick() + " - [" + user.getSellerRateMessage() + "]", Utils.toYuan(array[1]));
                            }
                            if (count++ > TOP_MAX) break;
                        }
                    }
                }

            }
        }

        //seller top 20
        if (true) {
            Map<String, List<String[]>> today = MapUtils.map(Utils.readInclude(input, new String[]{"tn_buyer"}));
            if (today.containsKey("tn_buyer")) {
                Map<String, List<String[]>> statusMap = MapUtils.map(today.get("tn_buyer"));
                for (String type : new String[]{"all"}) {
                    if (true) {
                        Table table = report.newTable("buyer_tn_top_" + type, typeMap.get(type) + "买家交易笔数TOP 100");
                        List<String[]> list = statusMap.get(type);
                        Collections.sort(list, new Comparator<String[]>() {
                            public int compare(String[] o1, String[] o2) {
                                return NumberUtils.toLong(o1[1]) < NumberUtils.toLong(o2[1]) ? 1 : -1;
                            }
                        });
                        int count = 0;
                        for (String[] array : list) {
                            if (null != array && array.length > 1) {
                                UserDO user = UicUtils.getUser(array[0]);
                                table.addCol(array[0],user.getNick() + " - [" + user.getSellerRateMessage() + "]", Utils.toYuan(array[1]));
                            }
                            if (count++ > TOP_MAX) break;
                        }
                    }
                }

            }
        }

        //商品TOP 20
        if (true) {
            Map<String, List<String[]>> today = MapUtils.map(Utils.readInclude(input, new String[]{"auction"}));
            if (today.containsKey("auction")) {
                Map<String, List<String[]>> statusMap = MapUtils.map(today.get("auction"));
                for (String type : new String[]{"all", "b2c", "c2c", "jhs"}) {
                    if (true) {
                        Table table = report.newTable("auction_top_" + type, typeMap.get(type) + "商品交易笔数TOP 100");
                        List<String[]> list = statusMap.get(type);
                        Collections.sort(list, new Comparator<String[]>() {
                            public int compare(String[] o1, String[] o2) {
                                return NumberUtils.toLong(o1[1]) < NumberUtils.toLong(o2[1]) ? 1 : -1;
                            }
                        });
                        int count = 0;
                        for (String[] array : list) {
                            if (null != array && array.length > 1) {
                                table.addCol(array[0], array[1]);
                            }
                            if (count++ > TOP_MAX) break;
                        }
                    }
                }

            }
        }

        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
    }


}
