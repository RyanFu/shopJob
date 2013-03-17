package org.dueam.hadoop.bp.report;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.dueam.hadoop.common.CounterMap;
import org.dueam.hadoop.common.util.Fmt;
import org.dueam.hadoop.common.util.MapUtils;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.hadoop.services.Category;
import org.dueam.hadoop.services.ShopDomain;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.commons.lang.math.NumberUtils.toDouble;
import static org.apache.commons.lang.math.NumberUtils.toLong;
import static org.dueam.hadoop.common.Functions.newHashMap;
import static org.dueam.hadoop.common.Functions.str;

/**
 * ������ĿIPV�����׶�ͳ��
 */
public class TotalCat {
    private static char CTRL_A = (char) 0x01;

    /**
     * @param args
     * @throws java.io.IOException
     */
    public static void main(String[] args) throws IOException {
        String input = args[0];
        if (!new File(input).exists()) {
            System.out.println("File Not Exist ! => " + input);
            return;
        }

        String mainName = args[1];

        Report report = Report.newReport(mainName + "�г����ҵ������");

        Map<String, List<String[]>> todayMap = MapUtils.map(Utils.read(args[0]), CTRL_A);
        Map<String, List<String[]>> ipvMap = MapUtils.map(todayMap.get("ipv"));
        Map<String, List<String[]>> tradeMap = MapUtils.map(todayMap.get("trade"));
        Map<String, String> orderCountMap = newHashMap();   //���ת���� IPV/֧��������UV
        Map<String, String> buyerUvMap = newHashMap();
        if (todayMap.containsKey("trade")) {
            Table tradeTable = report.newViewTable("total_trade", mainName + "���������Ϣһ����");
            tradeTable.addCol("��Ŀ").addCol("���׶Ԫ��").addCol("������").addCol("��ƷUV").addCol("����UV").addCol("�͵���").addCol(Report.BREAK_VALUE);
            Collections.sort(todayMap.get("trade"), new Comparator<String[]>() {
                public int compare(String[] o1, String[] o2) {
                    return NumberUtils.toLong(o1[3]) >= NumberUtils.toLong(o2[3]) ? -1 : 1;
                }
            });
            for (String[] _cols : todayMap.get("trade")) {
                String catId = "leafCat".equals(_cols[0]) ? _cols[2] : _cols[1];
                if (catId == null) catId = "NULL";
                orderCountMap.put(catId, _cols[8]);
                buyerUvMap.put(catId,_cols[6]) ;
                tradeTable.addCol(Category.getCategoryName(catId)).addCol(Fmt.moneyFmt(_cols[3]) + "(" + _cols[3] + ")").addCol(_cols[8]).addCol(_cols[5]).addCol(_cols[6]).addCol(Fmt.div(_cols[3], _cols[6])).addCol(Report.BREAK_VALUE);
            }
        }

        if (todayMap.containsKey("ipv")) {
            Table tradeTable = report.newViewTable("total_ipv", mainName + "��������Ϣһ����");
            tradeTable.addCol("��Ŀ").addCol("IPV").addCol("���UV").addCol("��ԱUV").addCol("IPV/���UV").addCol("���ת���ʣ�%��").addCol("�ɽ�ת���ʣ�%��").addCol(Report.BREAK_VALUE);
            Collections.sort(todayMap.get("ipv"), new Comparator<String[]>() {
                public int compare(String[] o1, String[] o2) {
                    return NumberUtils.toLong(o1[3]) >= NumberUtils.toLong(o2[3]) ? -1 : 1;
                }
            });
            for (String[] _cols : todayMap.get("ipv")) {
                String catId = "leafCat".equals(_cols[0]) ? _cols[2] : _cols[1];
                if (catId == null) catId = "NULL";
                tradeTable.addCol(Category.getCategoryName(catId)).addCol(Fmt.moneyFmt(_cols[3])).addCol(_cols[4]).addCol(_cols[5]).addCol(Fmt.div(_cols[3], _cols[4])).addCol(Fmt.parent2(orderCountMap.get(catId),_cols[3])).addCol(Fmt.parent2(buyerUvMap.get(catId),_cols[5])).addCol(Report.BREAK_VALUE);
            }
        }
        /**
         CREATE EXTERNAL TABLE trade_category (
         type string,
         root_cat string,
         cat_id string,
         total_fee double,
         discount_fee double,
         item_uv bigint COMMENT 'ÿ�ս�����ƷUV',
         buyer_uv bigint ,
         seller_uv bigint COMMENT '����û�UV')
         COMMENT '��Ŀ���ױ�'
         PARTITIONED BY(pt STRING)
         ROW FORMAT DELIMITED
         FIELDS TERMINATED BY '\t'
         STORED AS TEXTFILE;
         */
        if (tradeMap.containsKey("leafCat")) {
            Table totalFeeTable = report.newTable("trade_total_fee", mainName + "��������Ŀ֧�������׶�ͳ��");
            Table discountTable = report.newTable("trade_discount_fee", mainName + "��������Ŀ�ۿ۽��ͳ��");
            Table itemUvTable = report.newTable("trade_item_uv", mainName + "��������Ŀ������Ʒ��ͳ��");
            Table buyerUvTable = report.newTable("trade_buyer_uv", mainName + "��������Ŀ����UVͳ��");
            Table sellerUvTable = report.newTable("trade_seller_uv", mainName + "��������Ŀ����UVͳ��");
            for (String[] _cols : tradeMap.get("rootCat")) {
                String catId = _cols[0] == null ? "NULL" : _cols[0];
                totalFeeTable.addCol(catId, Category.getCategoryName(catId), _cols[2]);
                discountTable.addCol(catId, Category.getCategoryName(catId), _cols[3]);
                itemUvTable.addCol(catId, Category.getCategoryName(catId), _cols[4]);
                buyerUvTable.addCol(catId, Category.getCategoryName(catId), _cols[5]);
                sellerUvTable.addCol(catId, Category.getCategoryName(catId), _cols[6]);
            }
            for (String[] _cols : tradeMap.get("leafCat")) {
                String catId = _cols[1] == null ? "NULL" : _cols[1];
                totalFeeTable.addCol(catId, Category.getCategoryName(catId), _cols[2]);
                discountTable.addCol(catId, Category.getCategoryName(catId), _cols[3]);
                itemUvTable.addCol(catId, Category.getCategoryName(catId), _cols[4]);
                buyerUvTable.addCol(catId, Category.getCategoryName(catId), _cols[5]);
                sellerUvTable.addCol(catId, Category.getCategoryName(catId), _cols[6]);
            }
            totalFeeTable.sort(Table.SORT_VALUE);
            discountTable.sort(Table.SORT_VALUE);
            itemUvTable.sort(Table.SORT_VALUE);
            buyerUvTable.sort(Table.SORT_VALUE);
            sellerUvTable.sort(Table.SORT_VALUE);
        }
        /**
         * CREATE EXTERNAL TABLE ipv_category (
         type string,
         root_cat string,
         cat_id string,
         ipv bigint,
         muv bigint COMMENT '����UV',
         uv bigint COMMENT '����û�UV')
         COMMENT 'Ҷ����Ŀ��һ����Ŀ��IPV'
         PARTITIONED BY(pt STRING)
         ROW FORMAT DELIMITED
         FIELDS TERMINATED BY '\t'
         STORED AS TEXTFILE;
         */
        if (ipvMap.containsKey("leafCat")) {
            Table ipvTable = report.newTable("ipv_ipv", mainName + "��������ĿIPVͳ��");
            Table muvTable = report.newTable("ipv_muv", mainName + "��������Ŀ���UV");
            Table uvTable = report.newTable("ipv_uv", mainName + "��������Ŀ��ԱUV");
            for (String[] _cols : ipvMap.get("rootCat")) {
                String catId = _cols[0] == null ? "NULL" : _cols[0];
                ipvTable.addCol(catId, Category.getCategoryName(catId), _cols[2]);
                muvTable.addCol(catId, Category.getCategoryName(catId), _cols[3]);
                uvTable.addCol(catId, Category.getCategoryName(catId), _cols[4]);
            }
            for (String[] _cols : ipvMap.get("leafCat")) {
                String catId = _cols[1] == null ? "NULL" : _cols[1];
                ipvTable.addCol(catId, Category.getCategoryName(catId), _cols[2]);
                muvTable.addCol(catId, Category.getCategoryName(catId), _cols[3]);
                uvTable.addCol(catId, Category.getCategoryName(catId), _cols[4]);
            }
            ipvTable.sort(Table.SORT_VALUE);
            muvTable.sort(Table.SORT_VALUE);
            uvTable.sort(Table.SORT_VALUE);
        }

        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
    }


}
