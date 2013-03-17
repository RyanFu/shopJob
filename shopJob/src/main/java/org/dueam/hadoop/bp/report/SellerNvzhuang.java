package org.dueam.hadoop.bp.report;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.dueam.hadoop.common.Area;
import org.dueam.hadoop.common.tables.RFactSellPV;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.hadoop.services.Category;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Ůװ����ͳ��
 */
public class SellerNvzhuang {
    private static char CTRL_A = (char) 0x01;

    /**
     * @param args
     * @throws java.io.IOException
     */
    public static void main(String[] args) throws IOException {
        Report report = Report.newReport("Ůװ����ÿ��ͳ�Ʊ���");
        String input = args[0];
        if (!new File(input).exists()) {
            System.out.println("File Not Exist ! => " + input);
            return;
        }


        Area areaIpv = Area.newArea(0,1, 20, 50, 100, 200, 500, 1000, 2000, 5000);
        Area areaUv = Area.newArea(0,1, 20, 50, 100, 200, 500, 1000, 2000, 5000);
        Area areaZfbFee = Area.newArea(0,1, 20, 50, 100, 200, 500, 1000, 2000, 5000);
        Area areaZfbQuantity = Area.newArea(0,1, 2, 5, 10, 20, 50, 100, 200, 500);
        Area areaZfbUv = Area.newArea(0,1, 2, 5, 10, 20, 50, 100, 200, 500);
        Area areaItem = Area.newArea(0,1, 20, 50, 100, 200, 500, 1000, 2000, 5000);
        Area areaZfbFeeSum = Area.newArea(0,1, 20, 50, 100, 200, 500, 1000, 2000, 5000);
        Area areaZfbFeeCount = Area.newArea(0,1, 20, 50, 100, 200, 500, 1000, 2000, 5000);


        List<String> topList = new ArrayList<String>();
        for (String line : Utils.readWithCharset(input, "utf-8")) {
            String[] _allCols = StringUtils.splitPreserveAllTokens(line, CTRL_A);
            if (_allCols.length < 11) continue;
            areaIpv.count(NumberUtils.toDouble(_allCols[RFactSellPV.ipv]));
            areaUv.count(NumberUtils.toDouble(_allCols[RFactSellPV.uv]));
            areaZfbFee.count(NumberUtils.toDouble(_allCols[RFactSellPV.zfb_total_fee]));
            areaZfbQuantity.count(NumberUtils.toInt(_allCols[RFactSellPV.zfb_quantity]));
            areaItem.count(NumberUtils.toInt(_allCols[RFactSellPV.product_count]));
            areaZfbUv.count(NumberUtils.toDouble(_allCols[RFactSellPV.zfb_users]));

            long zfbTotalFee = (long)NumberUtils.toDouble(_allCols[RFactSellPV.zfb_total_fee]);
            long zfbQuantity = (NumberUtils.toLong(_allCols[RFactSellPV.zfb_quantity]));
            //ֻͳ���н��ף��ҿ͵���С��2W������
            if(zfbQuantity > 0 &&  (zfbTotalFee / zfbQuantity < 20000 )) {
                 areaZfbFeeSum.sum(zfbTotalFee);
                areaZfbFeeCount.count(NumberUtils.toDouble(_allCols[RFactSellPV.zfb_total_fee]));
            }


            if (NumberUtils.toInt(_allCols[RFactSellPV.alipay_rank]) < 1000) {
                topList.add(line);
            }
        }

        if (true) {
            Table table = report.newGroupTable("item", "������Ʒ���ֲ�ͳ��");
            for (String key : areaItem.areaKeys) {
                Long value = areaItem.get(key);
                if (value == null) value = 0L;
                table.addCol(key, String.valueOf(value));
            }
        }
        if (true) {
            Table table = report.newGroupTable("ipv", "����IPV�ֲ�ͳ��");
            for (String key : areaIpv.areaKeys) {
                Long value = areaIpv.get(key);
                if (value == null) value = 0L;
                table.addCol(key, String.valueOf(value));
            }
        }
        if (true) {
            Table table = report.newGroupTable("uv", "����UV�ֲ�ͳ��");
            for (String key : areaUv.areaKeys) {
                Long value = areaUv.get(key);
                if (value == null) value = 0L;
                table.addCol(key, String.valueOf(value));
            }
        }
        if (true) {
            Table table = report.newGroupTable("zfb_fee", "����֧�������׶Ԫ���ֲ�ͳ��");
            for (String key : areaZfbFee.areaKeys) {
                Long value = areaZfbFee.get(key);
                if (value == null) value = 0L;
                table.addCol(key, String.valueOf(value));
            }
        }

        if (true) {
            Table table = report.newGroupTable("zfb_fee_sum", "����֧�������׶Ԫ���ֲ���������ܣ����˵��󶩵����޽������ң�");
            for (String key : areaZfbFeeSum.areaKeys) {
                Long value = areaZfbFeeSum.get(key);
                if (value == null) value = 0L;
                table.addCol(key, String.valueOf(value));
            }
        }
        if (true) {
            Table table = report.newGroupTable("zfb_fee_count", "����֧�������׶Ԫ���ֲ�����ͳ�ƣ����˵��󶩵����޽������ң�");
            for (String key : areaZfbFeeCount.areaKeys) {
                Long value = areaZfbFeeCount.get(key);
                if (value == null) value = 0L;
                table.addCol(key, String.valueOf(value));
            }
        }
        if (true) {
            Table table = report.newGroupTable("zfb_quantity", "����֧���������ֲ�ͳ��");
            for (String key : areaZfbQuantity.areaKeys) {
                Long value = areaZfbQuantity.get(key);
                if (value == null) value = 0L;
                table.addCol(key, String.valueOf(value));
            }
        }

        if (true) {
            Table table = report.newGroupTable("zfb_uv", "����֧�������������ֲ�ͳ��");
            for (String key : areaZfbUv.areaKeys) {
                Long value = areaZfbUv.get(key);
                if (value == null) value = 0L;
                table.addCol(key, String.valueOf(value));
            }
        }


        Collections.sort(topList, new Comparator<String>() {
            public int compare(String o1, String o2) {
                String[] _allCols1 = StringUtils.splitPreserveAllTokens(o1, CTRL_A);
                String[] _allCols2 = StringUtils.splitPreserveAllTokens(o2, CTRL_A);
                return NumberUtils.toInt(_allCols1[RFactSellPV.alipay_rank]) - NumberUtils.toInt(_allCols2[RFactSellPV.alipay_rank]);  //To change body of implemented methods use File | Settings | File Templates.
            }
        });

        if (true) {
            Table table = report.newViewTable("top", "TOP 100 ֧�������׶�����");
            table.addCol(null, "�ǳ�");
            table.addCol(null, "����");
            table.addCol(null, "���׶�");
            table.addCol(null, "���ױ���");
            table.addCol(null, "��������");
            table.addCol(null, "IPV");
            table.addCol(null, "UV");
            table.addCol(Report.newBreakValue());
            for (String line : topList) {
                String[] _allCols = StringUtils.splitPreserveAllTokens(line, CTRL_A);
                table.addCol(null, _allCols[RFactSellPV.nick]);
                table.addCol("top_rank_"+_allCols[RFactSellPV.id],"֧�������׶�����", _allCols[RFactSellPV.alipay_rank]);
                table.addCol("top_zfb_fee_"+_allCols[RFactSellPV.id],"֧�������׶�", String.valueOf((long)NumberUtils.toDouble(_allCols[RFactSellPV.zfb_total_fee])));
                table.addCol("top_zfb_quantity_"+_allCols[RFactSellPV.id], "֧�������ױ���",_allCols[RFactSellPV.zfb_quantity]);
                table.addCol("top_zfb_uv_"+_allCols[RFactSellPV.id],"֧������������", _allCols[RFactSellPV.zfb_users]);
                table.addCol("top_ipv_"+_allCols[RFactSellPV.id],"����IPV", _allCols[RFactSellPV.ipv]);
                table.addCol("top_uv_"+_allCols[RFactSellPV.id],"����UV", _allCols[RFactSellPV.uv]);
                table.addCol(Report.newBreakValue());
            }


        }

        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
    }
}
