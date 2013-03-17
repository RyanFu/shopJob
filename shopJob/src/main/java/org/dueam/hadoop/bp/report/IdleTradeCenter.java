package org.dueam.hadoop.bp.report;

import org.apache.commons.lang.math.NumberUtils;
import org.dueam.hadoop.common.util.Fmt;
import org.dueam.hadoop.common.util.MapUtils;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.hadoop.services.Category;
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
 * Date: 10-12-20 ����6:08
 */
public class IdleTradeCenter {
    public static void main(String[] args) throws Exception {
        String input = args[0];
        if (!new File(input).exists()) {
            return;
        }

        Map<String, List<String[]>> today = MapUtils.map(Utils.read(input, null));
        org.dueam.report.common.Report report = org.dueam.report.common.Report.newReport("�������ý��ױ���");
        //group by source
        commonReport(false, report, "paid", "֧�����ɽ������Դ���֣�", "��Դ(��λ��Ԫ)", new Callback() {
            public String call(String key) {
                return tradeSource(key);
            }
        }, today.get("paid"));

        if (true) {
            Table table = report.getTable("paid");
            if (today.containsKey("buyer") && table != null)
                table.addCol("paid_uv", "֧��������UV", String.valueOf(today.get("buyer").size()));
        }

        //group by biz type
        commonReport(true, report, "bizType", "�ɽ�����������ͻ��֣�", "ͳ������(��λ��Ԫ)", new Callback() {
            public String call(String key) {
                return getBizType(key);
            }
        }, today.get("bizType"));

        //�˿���ص�ͳ��
        if (true) {
            if (today.containsKey("refund")) {
                Map<String, String> statusMap = MapUtils.toSimpleMap(today.get("refund"));
                Table table = report.newTable("refund_detail", "������ɽ��׵��Ӷ���ͳ��");
                for (Map.Entry<String, String> entry : REFUND_KEY_TABLE.entrySet()) {
                    if (statusMap.containsKey(entry.getKey())) {
                        if (entry.getKey().endsWith("sum")) {
                            table.addCol(entry.getKey(), entry.getValue(), Utils.toYuan(statusMap.get(entry.getKey())));
                        } else {
                            table.addCol(entry.getKey(), entry.getValue(), statusMap.get(entry.getKey()));
                        }
                    }
                }
            }

        }


        sellerTrade(report, today.get("seller"));

        buyerTrade(report, today.get("buyer"));

        //group by biz category
        commonReport(true, report, "cat", "�ɽ����һ����Ŀ���֣�", "ͳ������(��λ��Ԫ)", new Callback() {
            public String call(String key) {
                return Category.get(key);
            }
        }, today.get("cat"));

        //group by source
        commonReport(false, report, "gmv", "GMV�ɽ������Դ���֣�", "��Դ(��λ��Ԫ)", new Callback() {
            public String call(String key) {
                return tradeSource(key);
            }
        }, today.get("gmv"));

        if (true) {
            Table table = report.getTable("gmv");
            if (today.containsKey("gmv_buyer") && table != null)
                table.addCol("gmb_uv", "GMV-UV", String.valueOf(today.get("gmv_buyer").size()));
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
            Table table = report.newGroupTable("seller_paid", "����ÿ��֧�������׶��������");
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
            Table table = report.newGroupTable("seller_trade_num", "����ÿ��֧�������ױ����������");

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
            Table table = report.newGroupTable("buyer_paid", "���ÿ��֧�������׶��������");
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
            Table table = report.newGroupTable("buyer_paid_num", "���ÿ��֧�������ױ����������");
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

    public static void commonReport(boolean isGroup, org.dueam.report.common.Report report, String key, String name, String firstCol, Callback callback, List<String[]> today) {
        if (today == null) return;
        Table table = report.newTable(key, name, "γ��", firstCol);
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
            //ת��Ԫ��ʾ
            if("num".equals(_key)){
                table.addCol(org.dueam.report.common.Report.newValue(_key, firstName, String.valueOf(todayTrade)));
            }   else {
                table.addCol(org.dueam.report.common.Report.newValue(_key, firstName, String.valueOf(todayTrade / 100)));
            }
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
            return "ȫ��";
        } else if ("item".equals(from)) {
            return "һ�ڼ���Ʒ";
        } else if ("c2c".equals(from)) {
            return "C2C";
        } else if ("cart".equals(from)) {
            return "���ﳵ";
        } else if ("b2c".equals(from)) {
            return "�Ա��̳�";
        } else if ("wap".equals(from)) {
            return "�ֻ��Ա�";
        } else if ("fbuy".equals(from)) {
            return "�ÿ�";
        } else if ("auction".equals(from)) {
            return "����";
        } else if ("jhs".equals(from)) {
            return "�ۻ���";
        } else if ("dpc_jx".equals(from)) {
            return "DPC_����";
        } else if ("dpc_dx".equals(from)) {
            return "DPC_����";
        }  else if ("num".equals(from)) {
            return "���ױ���";
        }
        return from;


    }

    private static Map<String, String> BIZ_TYPE_MAP = MapUtils.asMap(new String[]{"100", "ֱ��",
            "200", "�Ź�����һ�ڼ�",
            "300", "�Զ�����",
            "500", "��������",
            "600", "�ⲿ����(��׼��)",
            "610", "�ⲿ�������Ű�",
            "620", "�ⲿ����(shopEX��)",
            "630", "�Ա���������������",
            "650", "�ⲿ����ͳһ��",
            "700", "���β�Ʒ",
            "710", "�Ƶ�",
            "800", "����ƽ̨(�ɹ���)",
            "900", "�������⽻��",
            "1000", "opensearch����",
            "1100", "���ս���",
            "1001", "opensearch COD����"
    });

    private static Map<String, String> REFUND_KEY_TABLE = MapUtils.asMap(new String[]{
            "total_num", "������ɵĽ��ף�������",
            "total_sum", "������ɵĽ��ף���",
            "success_num", "���׳ɹ���������",
            "success_sum", "���׳ɹ�����",
            "close_num", "�رս��ף�������",
            "close_sum", "�رս��ף���",
            "refund_num", "�����˿�Ľ��ף�������",
            "refund_sum", "�˿���"
    });

    public static String getBizType(String type) {
        String value = BIZ_TYPE_MAP.get(type);
        return (value == null ? "" : value) + "[" + type + "]";
    }


}