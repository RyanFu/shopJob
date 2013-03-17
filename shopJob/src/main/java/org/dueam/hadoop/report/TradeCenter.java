package org.dueam.hadoop.report;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.dueam.hadoop.common.tables.TcBizOrder;
import org.dueam.hadoop.common.util.*;
import org.dueam.hadoop.services.Category;

import java.io.File;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * User: windonly
 * Date: 10-12-20 ����6:08
 */
public class TradeCenter {
    public static void main(String[] args) throws Exception {
        String input = args[0];
        if(!new File(input).exists()) {
            return;
        }

        Map<String, List<String[]>> today = MapUtils.map(Utils.read(input,(String[])null));
        Map<String, List<String[]>> yesterday = MapUtils.map(Utils.read(DateStringUtils.add(input, -1),new String[]{"seller","buyer"}));
        Map<String, List<String[]>> lastWeek = MapUtils.map(Utils.read(DateStringUtils.add(input, -7),new String[]{"seller","buyer"}));

        //group by source
        commonReport("֧�����ɽ������Դ���֣�", "��Դ(��λ��Ԫ)", new Callback() {
            public String call(String key) {
                return tradeSource(key);
            }
        }, today.get("paid"), yesterday.get("paid"), lastWeek.get("paid"));

        //group by biz type
        commonReport("�ɽ�����������ͻ��֣�", "ͳ������(��λ��Ԫ)", new Callback() {
            public String call(String key) {
                return TcBizOrder.getBizType(key);
            }
        }, today.get("bizType"), yesterday.get("bizType"), lastWeek.get("bizType"));

        sellerTrade(today.get("seller"));

        buyerTrade(today.get("buyer"));

        //group by biz category
        commonReport("�ɽ����һ����Ŀ���֣�", "ͳ������(��λ��Ԫ)", new Callback() {
            public String call(String key) {
                return Category.get(key);
            }
        }, today.get("cat"), yesterday.get("cat"), lastWeek.get("cat"));

        //group by source
        commonReport("GMV�ɽ������Դ���֣�", "��Դ(��λ��Ԫ)", new Callback() {
            public String call(String key) {
                return tradeSource(key);
            }
        }, today.get("gmv"), yesterday.get("gmv"), lastWeek.get("gmv"));


        FileUtils.writeStringToFile(new File(args[0] + ".html"), Report.renderHtml(null), "GBK");

    }

    private static void sellerTrade(List<String[]> today) {
        if (today == null) return;
        long[] trade = new long[today.size()];
        long[] count = new long[today.size()];
        int pos = 0;
        for (String[] value : today) {
            trade[pos] = NumberUtils.toLong(value[1], 0);
            count[pos] = NumberUtils.toLong(value[2], 0);
            pos++;
        }
        long[] tradeArea = new long[]{0, 200 * 100, 500 * 100, 1000 * 100,2000 * 100,5000 * 100, 10000 * 100, 1000000 * 10, 1000000 * 50, 1000000 * 100};
        long[] tradeSum = Utils.count(trade, tradeArea);
        Report.newTable("����ÿ��֧�������׶��������", null, null);
        long sum = Utils.sum(tradeSum);
        Report.add(new String[]{"����", "����", "�ٷֱ�"});
        for (int i = 0; i < tradeArea.length; i++) {
            String key = null;
            if (i == tradeArea.length - 1) {
                key = Fmt.money(tradeArea[i]) + " ~ ";
            } else {
                key = Fmt.money(tradeArea[i]) + " ~ " + Fmt.money(tradeArea[i + 1]);
            }
            Report.add(new String[]{key, String.valueOf(tradeSum[i]), Fmt.parent(tradeSum[i], sum)});
        }

        long[] countArea = new long[]{0, 2, 5, 10, 100, 200, 500, 1000};
        long[] countSum = Utils.count(count, countArea);
        Report.newTable("����ÿ��֧�������ױ����������", null, null);

        sum = Utils.sum(countSum);
        Report.add(new String[]{"����", "����", "�ٷֱ�"});
        for (int i = 0; i < countArea.length; i++) {
            String key = null;
            if (i == countArea.length - 1) {
                key = countArea[i] + " ~ ";
            } else {
                key = countArea[i] + " ~ " + (countArea[i + 1] - 1);
            }
            Report.add(new String[]{key, String.valueOf(countSum[i]), Fmt.parent(countSum[i], sum)});
        }

    }


    private static void buyerTrade(List<String[]> today) {
        if (today == null) return;
        long[] trade = new long[today.size()];
        long[] count = new long[today.size()];
        int pos = 0;
        for (String[] value : today) {
            trade[pos] = NumberUtils.toLong(value[1], 0);
            count[pos] = NumberUtils.toLong(value[2], 0);
            pos++;
        }
        long[] tradeArea = new long[]{0,10 * 100, 20 * 100, 50 * 100, 100 * 100, 200 * 100, 500 * 100, 1000 * 100, 2000 * 100, 5000 * 100};
        long[] tradeSum = Utils.count(trade, tradeArea);
        Report.newTable("���ÿ��֧�������׶��������", null, null);
        long sum = Utils.sum(tradeSum);
        Report.add(new String[]{"����", "����", "�ٷֱ�"});
        for (int i = 0; i < tradeArea.length; i++) {
            String key = null;
            if (i == tradeArea.length - 1) {
                key = Fmt.money(tradeArea[i]) + " ~ ";
            } else {
                key = Fmt.money(tradeArea[i]) + " ~ " + Fmt.money(tradeArea[i + 1]);
            }
            Report.add(new String[]{key, String.valueOf(tradeSum[i]), Fmt.parent(tradeSum[i], sum)});
        }

        long[] countArea = new long[]{0, 2, 5, 10, 50};
        long[] countSum = Utils.count(count, countArea);
        Report.newTable("���ÿ��֧�������ױ����������", null, null);

        sum = Utils.sum(countSum);
        Report.add(new String[]{"����", "����", "�ٷֱ�"});
        for (int i = 0; i < countArea.length; i++) {
            String key = null;
            if (i == countArea.length - 1) {
                key = countArea[i] + " ~ ";
            } else {
                key = countArea[i] + " ~ " + (countArea[i + 1] - 1);
            }
            Report.add(new String[]{key, String.valueOf(countSum[i]), Fmt.parent(countSum[i], sum)});
        }

    }

    public static void commonReport(String name, String firstCol, Callback callback, List<String[]> today, List<String[]> yesterday, List<String[]> lastWeek) {
        if (today == null) return;
        Report.Table table = Report.newTable(name, null, null);
        Collections.sort(today, new Comparator<String[]>() {
            public int compare(String[] o1, String[] o2) {
                return (NumberUtils.toLong(o1[1]) < NumberUtils.toLong(o2[1])) ? 1 : 0;
            }

        });
        table.add(new String[]{firstCol, "����", "����", "ͬ�������", "����", "ͬ�������"});
        Map<String, String[]> todayMap = MapUtils.toMap(today);
        Map<String, String[]> yesterdayMap = MapUtils.toMap(yesterday);
        Map<String, String[]> lastWeekMap = MapUtils.toMap(lastWeek);
        for (String key : todayMap.keySet()) {
            String firstName = callback.call(key);
            long todayTrade = get(todayMap.get(key));
            long yesterdayTrade = get(yesterdayMap.get(key));
            long lastWeekTrade = get(lastWeekMap.get(key));
            table.add(new String[]{firstName, Fmt.money(todayTrade), Fmt.money(yesterdayTrade), Fmt.growing(todayTrade, yesterdayTrade), Fmt.money(lastWeekTrade), Fmt.growing(todayTrade, lastWeekTrade)});
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
        }
        return from;


    }

}
