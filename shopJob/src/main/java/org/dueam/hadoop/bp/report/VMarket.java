package org.dueam.hadoop.bp.report;

import org.apache.commons.lang.StringUtils;
import org.dueam.hadoop.common.util.Fmt;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Ůװ����ͳ��
 */
public class VMarket {
    private static char CTRL_A = (char) 0x01;
    static Map<String, String> typeMap = Utils.asLinkedMap("1410", "��������", "1474", "��ͷ����", "1538", "����ͨ��", "1602", "��MMװ", "1666", "������װ", "1858", "��ɴ���", "1730","ְҵ��װ", "1794", "��̨��װ");

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

        Report report = Report.newReport("���� & ��ɫ���������������ͳ��");
        Table main = report.newViewTable("total", "�������ݻ�����Ϣ");
        main.addCol("�ݷ���").addCol("������Ʒ��").addCol("���׶�")
                .addCol("����UV").addCol("IPV").addCol("IPV-UV").addCol("����ת����").addCol(Report.BREAK_VALUE);
        Map<String, Table> tableMap = new LinkedHashMap<String, Table>();
        Map<String, String[]> totalMap = new HashMap<String, String[]>();
        for (String key : typeMap.keySet()) {
            tableMap.put(key, report.newTable("table_" + key, typeMap.get(key) + "��"));
            totalMap.put(key, new String[7]);
            totalMap.get(key)[0] = typeMap.get(key) + "��";
        }

        for (String line : Utils.readWithCharset(input, "utf-8")) {
            String[] _allCols = StringUtils.splitPreserveAllTokens(line, CTRL_A);
            String type = _allCols[0];
            String tag = _allCols[1];
            Table table = tableMap.get(tag);
            if ("item".equals(type)) {
                //����
                String namePrefix = "1".equals(_allCols[2]) ? "����" : "�ֿ���";
                table.addCol(_allCols[2] + "_item_count", namePrefix + "��Ʒ��", _allCols[3]);
                table.addCol(_allCols[2] + "_item_count_sold", namePrefix + "��Ʒ��(������)", _allCols[4]);
                table.addCol(_allCols[2] + "_item_count_hb", namePrefix + "��Ʒ��(�л���)", _allCols[5]);
                if ("1".equals(_allCols[2])) {
                    totalMap.get(tag)[1] = _allCols[3];
                }
            } else if ("ipv".equals(type)) {
                table.addCol("IPV", _allCols[2]);
                totalMap.get(tag)[4] = _allCols[2];
                table.addCol("IPV-UV", _allCols[3]);
                totalMap.get(tag)[5] = _allCols[3];
                table.addCol("IPV-UV(MID)", _allCols[4]);
            } else if ("trade".equals(type)) {
                table.addCol("TRADE_TOTAL_FEE", "֧�������׶�", _allCols[2]);
                totalMap.get(tag)[2] = Fmt.moneyFmt(_allCols[2]);
                table.addCol("TRADE-ITEM-UV", "�н��׵���Ʒ��", _allCols[3]);
                table.addCol("TRADE-BUYER-UV", "����UV", _allCols[4]);
                totalMap.get(tag)[3] = _allCols[4];
                table.addCol("TRADE-SELLER-UV", "�������׵�������", _allCols[5]);
            }

        }

        for (String[] cos : totalMap.values()) {
            cos[6] = Fmt.parent2(cos[3], cos[5]);
        }
        for (String[] cos : totalMap.values()) {
            for (String c : cos) {
                main.addCol(c);
            }
            main.addCol(Report.BREAK_VALUE);
        }


        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
    }

}
