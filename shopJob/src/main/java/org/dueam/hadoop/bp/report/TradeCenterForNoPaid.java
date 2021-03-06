package org.dueam.hadoop.bp.report;

import org.apache.commons.lang.math.NumberUtils;
import org.dueam.hadoop.common.util.Fmt;
import org.dueam.hadoop.common.util.MapUtils;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.hadoop.services.Category;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * User: windonly
 * Date: 10-12-20 下午6:08
 */
public class TradeCenterForNoPaid {
    public static void main(String[] args) throws Exception {
        String input = args[0];
        if (!new File(input).exists()) {
            return;
        }
        Report report = Report.newReport("未付款订单交易报表");
        if (true) {

            Map<String, List<String[]>> today = MapUtils.map(Utils.read(input, new String[]{"seller", "buyer"}));

            //group by source
            commonReport(false, true, report, "gmv", "GMV成交额（按来源划分）", "来源(单位：元)", new Callback() {
                        public String call(String key) {
                            return tradeSource(key);
                        }
                    }, today.get("gmv"));

            //group by biz type
            commonReport(true, true, report, "bizType", "成交额（按交易类型划分）", "统计类型(单位：元)", new Callback() {
                        public String call(String key) {
                            return getBizType(key);
                        }
                    }, today.get("bizType"));
            //group by biz type
            commonReport(true, false, report, "bizTypeNum", "交易笔数（按交易类型划分）", "数量", new Callback() {
                        public String call(String key) {
                            return getBizType(key);
                        }
                    }, today.get("bizTypeNum"));

            //group by biz category
            commonReport(true, true, report, "cat", "成交额（按一级类目划分）", "统计类型(单位：元)", new Callback() {
                        public String call(String key) {
                            return Category.get(key);
                        }
                    }, today.get("cat"));

            commonReport(true, false, report, "catNum", "成交笔数（按一级类目划分）", "数量", new Callback() {
                        public String call(String key) {
                            return Category.get(key);
                        }
                    }, today.get("catNum"));
            today = null;
        }
        if (true) {
            Map<String, List<String[]>> tmpMap = MapUtils.map(Utils.readInclude(input, new String[]{"seller"}));
            sellerTrade(report, tmpMap.get("seller"));
            tmpMap = null;
        }

        if (true) {
            Map<String, List<String[]>> tmpMap = MapUtils.map(Utils.readInclude(input, new String[]{"buyer"}));
            buyerTrade(report, tmpMap.get("buyer"));
            tmpMap = null;
        }


        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
    }

    private static void sellerTrade(org.dueam.report.common.Report report, List<String[]> today) {
        if (today == null) return;
        long[] trade = new long[today.size()];
        long[] count = new long[today.size()];
        int pos = 0;
        for (String[] value : today) {
            trade[pos] = NumberUtils.toLong(value[1], 0);
            count[pos] = NumberUtils.toLong(value[2], 0);
            pos++;
        }

        if (true) {
            long[] tradeArea = new long[]{0, 200 * 100, 500 * 100, 1000 * 100, 2000 * 100, 5000 * 100, 10000 * 100, 1000000 * 10, 1000000 * 50, 1000000 * 100};
            long[] tradeSum = Utils.count(trade, tradeArea);
            Table table = report.newGroupTable("seller_paid", "卖家每日支付宝交易额区间汇总");
            long sum = Utils.sum(tradeSum);
            for (int i = 0; i < tradeArea.length; i++) {
                String key = null;
                if (i == tradeArea.length - 1) {
                    key = Fmt.money(tradeArea[i]) + " ~";
                } else {
                    key = Fmt.money(tradeArea[i]) + " ~ " + Fmt.money(tradeArea[i + 1]);
                }
                table.addCol(org.dueam.report.common.Report.newValue(key, String.valueOf(tradeSum[i])));
            }
        }


        if (true) {
            long[] countArea = new long[]{0, 2, 5, 10, 100, 200, 500, 1000};
            long[] countSum = Utils.count(count, countArea);
            Table table = report.newGroupTable("seller_trade_num", "卖家每日支付宝交易笔数区间汇总");

            long sum = Utils.sum(countSum);
            for (int i = 0; i < countArea.length; i++) {
                String key = null;
                if (i == countArea.length - 1) {
                    key = countArea[i] + " ~";
                } else {
                    key = countArea[i] + " ~ " + (countArea[i + 1] - 1);
                }
                table.addCol(org.dueam.report.common.Report.newValue(key, String.valueOf(countSum[i])));
            }
        }
    }


    private static void buyerTrade(org.dueam.report.common.Report report, List<String[]> today) {
        if (today == null) return;
        long[] trade = new long[today.size()];
        long[] count = new long[today.size()];
        int pos = 0;
        for (String[] value : today) {
            trade[pos] = NumberUtils.toLong(value[1], 0);
            count[pos] = NumberUtils.toLong(value[2], 0);
            pos++;
        }

        if (true) {
            long[] tradeArea = new long[]{0, 10 * 100, 20 * 100, 50 * 100, 100 * 100, 200 * 100, 500 * 100, 1000 * 100, 2000 * 100, 5000 * 100};
            long[] tradeSum = Utils.count(trade, tradeArea);
            Table table = report.newGroupTable("buyer_paid", "买家每日支付宝交易额区间汇总");
            long sum = Utils.sum(tradeSum);
            for (int i = 0; i < tradeArea.length; i++) {
                String key = null;
                if (i == tradeArea.length - 1) {
                    key = Fmt.money(tradeArea[i]) + " ~";
                } else {
                    key = Fmt.money(tradeArea[i]) + " ~ " + Fmt.money(tradeArea[i + 1]);
                }
                table.addCol(org.dueam.report.common.Report.newValue(key, String.valueOf(tradeSum[i])));
            }
        }

        if (true) {
            long[] countArea = new long[]{0, 2, 5, 10, 50};
            long[] countSum = Utils.count(count, countArea);
            Table table = report.newGroupTable("buyer_paid_num", "买家每日支付宝交易笔数区间汇总");
            long sum = Utils.sum(countSum);
            for (int i = 0; i < countArea.length; i++) {
                String key = null;
                if (i == countArea.length - 1) {
                    key = countArea[i] + " ~";
                } else {
                    key = countArea[i] + " ~ " + (countArea[i + 1] - 1);
                }
                table.addCol(org.dueam.report.common.Report.newValue(key, String.valueOf(countSum[i])));
            }
        }

    }

    public static void commonReport(boolean isGroup, boolean ymb, org.dueam.report.common.Report report, String key, String name, String firstCol, Callback callback, List<String[]> today) {
        if (today == null) return;
        Table table = report.newTable(key, name, "纬度", firstCol);
        if (isGroup) {
            table.setType(Table.GROUP_TYPE);
        }
        Collections.sort(today, new Comparator<String[]>() {
            public int compare(String[] o1, String[] o2) {
                return (NumberUtils.toLong(o1[1]) < NumberUtils.toLong(o2[1])) ? 1 : 0;
            }

        });
        Map<String, String[]> todayMap = MapUtils.toMap(today);
        for (String _key : todayMap.keySet()) {
            String firstName = callback.call(_key);
            long todayTrade = get(todayMap.get(_key));
            if (ymb) {
                todayTrade = todayTrade / 100;
            }
            //转成元显示
            table.addCol(org.dueam.report.common.Report.newValue(_key, firstName, String.valueOf(todayTrade)));

        }


    }

    public static long get(String[] array) {
        if (array == null) return 0;
        return NumberUtils.toLong(array[0], 0);
    }

    private interface Callback {
        String call(String key);
    }

    private static String tradeSource(String from) {
        if ("all".equals(from)) {
            return "全部";
        } else if ("item".equals(from)) {
            return "一口价商品";
        } else if ("c2c".equals(from)) {
            return "C2C";
        } else if ("cart".equals(from)) {
            return "购物车";
        } else if ("b2c".equals(from)) {
            return "淘宝商城";
        } else if ("wap".equals(from)) {
            return "手机淘宝";
        } else if ("fbuy".equals(from)) {
            return "访客";
        } else if ("auction".equals(from)) {
            return "拍卖";
        } else if ("jhs".equals(from)) {
            return "聚划算";
        } else if ("dpc_jx".equals(from)) {
            return "DPC_经销";
        } else if ("dpc_dx".equals(from)) {
            return "DPC_代销";
        }
        return from;


    }

    private static Map<String, String> BIZ_TYPE_MAP = MapUtils.asMap(new String[]{"100", "直冲",
            "200", "团购拍卖一口价",
            "300", "自动发货",
            "500", "货到付款",
            "600", "外部网店(标准版)",
            "610", "外部网店入门版",
            "620", "外部网店(shopEX版)",
            "630", "淘宝与万网合作版网",
            "650", "外部网店统一版",
            "700", "旅游产品",
            "710", "酒店",
            "800", "分销平台(采购单)",
            "900", "网游虚拟交易",
            "1000", "opensearch交易",
            "1100", "保险交易",
            "1001", "opensearch COD交易"
    });

    public static String getBizType(String type) {
        String value = BIZ_TYPE_MAP.get(type);
        return (value == null ? "" : value) + "[" + type + "]";
    }


}