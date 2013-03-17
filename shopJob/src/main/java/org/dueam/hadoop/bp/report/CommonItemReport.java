package org.dueam.hadoop.bp.report;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.dueam.hadoop.common.util.Fmt;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

/**
 * ͨ����Ʒ����
 */
public class CommonItemReport {
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

        String name = "";
        if (args.length > 1) {
            name = args[1];
        }

        Report report = Report.newReport(name + "��Ʒ���ױ���");
        Map<String, Table> summayMap = new HashMap<String, Table>();
        summayMap.put("all", report.newTable("allsummay", "ȫ��--��������"));

        for (String line : Utils.readWithCharset(input, "utf-8")) {
            String[] allCols = StringUtils.splitPreserveAllTokens(line, CTRL_A);

            if ("summay".equals(allCols[0])) {

                Table summay = getTable(summayMap, allCols[1], report);

                summay.addCol("��Ʒ��", allCols[2]);
                summay.addCol("������Ʒ��", allCols[3]);
                summay.addCol("�·���Ʒ��", allCols[4]);
                summay.addCol("������Ʒ��", allCols[5]);
                continue;
            }

            if ("ipv".equals(allCols[0])) {

                Table summay = getTable(summayMap, allCols[1], report);

                summay.addCol("IPV", allCols[2]);
                summay.addCol("IPV-UV", allCols[3]);
                continue;
            }

            if ("trade".equals(allCols[0])) {

                Table summay = getTable(summayMap, allCols[1], report);

                if (summay == null) {
                    continue;
                }

                summay.addCol("֧�������", StringUtils.isNumeric(allCols[2]) ? allCols[2] : "0");
                summay.addCol("�����������Ʒ��", allCols[3]);
                summay.addCol("��������������", allCols[4]);
                summay.addCol("���������������", allCols[5]);
                summay.addCol("������", allCols[6]);
                summay.addCol("�͵���", Fmt.parent4(allCols[2], allCols[4]));
                continue;
            }

            if ("memeber_cart".equals(allCols[0])) {

                Table summay = getTable(summayMap, allCols[1], report);

                if (summay == null) {
                    continue;
                }

                summay.addCol("���빺�ﳵ�ܴ���(��Ա)", allCols[2]);
                continue;
            }

            if ("guest_cart".equals(allCols[0])) {

                Table summay = getTable(summayMap, allCols[1], report);

                if (summay == null) {
                    continue;
                }
                summay.addCol("���빺�ﳵ�ܴ���(�ÿ�)", allCols[2]);
                continue;
            }

        }

        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
    }

    private static Table getTable(Map<String, Table> summayMap, String catId, Report report) {

        Table summay = summayMap.get(catId);

        String catName = catMap.get(catId);

        if (summay == null) {
            summay = report.newTable(catId + "summay", catName == null ? catId : catName + "--��������");
            summayMap.put(catId, summay);
        }
        return summay;
    }

    /**
     * һ����Ŀmap
     */
    private static Map<String, String> catMap = new HashMap<String, String>();

    static {

        catMap.put("40", "��ѶQQר��");
        catMap.put("1512", "�ֻ�");
        catMap.put("50008163", "������Ʒ/��������");
        catMap.put("50011397", "�鱦/��ʯ/���/�ƽ�");
        catMap.put("50016348", "���/��ԡ/����/�����þ�");
        catMap.put("50018222", "̨ʽ��/һ���/������");
        catMap.put("50016349", "����/�����þ�");
        catMap.put("50013886", "����/��ɽ/ҰӪ/������Ʒ");
        catMap.put("50020276", "Ʒ�Ʊ���Ʒ");
        catMap.put("50019379", "�����̼�");
        catMap.put("50020808", "�Ҿ���Ʒ");
        catMap.put("50025705", "ϴ������/������/ֽ/��޹");
        catMap.put("50023904", "������Ʒ����");
        catMap.put("50025004", "���Զ���/��Ʒ���/DIY");
        catMap.put("50025111", "���ػ��������");
        catMap.put("50025490", "���������Ź�");
        catMap.put("50025706", "�����");
        catMap.put("29", "����/����ʳƷ����Ʒ");
        catMap.put("1201", "MP3/MP4/iPod/¼����");
        catMap.put("1101", "�ʼǱ�����");
        catMap.put("34", "����/Ӱ��/����/����");
        catMap.put("33", "�鼮/��־/��ֽ");
        catMap.put("30", "��װ");
        catMap.put("28", "ZIPPO/��ʿ����/�۾�");
        catMap.put("26", "����/��Ʒ/���/��װ/Ħ��");
        catMap.put("99", "������Ϸ�㿨");
        catMap.put("50011699", "�˶���/�˶���/�������");
        catMap.put("50008075", "�Ժ������ۿ�ȯ");
        catMap.put("50010788", "��ױ/��ˮ/��ױ����");
        catMap.put("50008907", "�ֻ�����/�ײ�/��ֵҵ��");
        catMap.put("50008164", "סլ�Ҿ�");
        catMap.put("50018004", "���Ӵʵ�/��ֽ��/�Ļ���Ʒ");
        catMap.put("50018252", "����ƾ֤");
        catMap.put("50022703", "��ҵ�");
        catMap.put("50023282", "��������/�ٷ�");
        catMap.put("50019780", "ƽ�����/MID");
        catMap.put("50020332", "��������");
        catMap.put("50020857", "��ɫ�ֹ���");
        catMap.put("50024186", "����");
        catMap.put("50023717", "OTCҩƷ/ҽ����е/�����۾�/������Ʒ");
        catMap.put("50024612", "����/����/���ͷ��񣨴�ֱ�г���");
        catMap.put("2813", "������Ʒ/����/������Ʒ");
        catMap.put("35", "�̷�/��ʳ/Ӫ��Ʒ");
        catMap.put("23", "�Ŷ�/�ʱ�/�ֻ�/�ղ�");
        catMap.put("21", "�Ӽ�����/����/������Ʒ");
        catMap.put("20", "����/���/��Ϸ/����");
        catMap.put("16", "Ůװ/Ůʿ��Ʒ");
        catMap.put("14", "�������/�������/�����");
        catMap.put("11", "����Ӳ��/��ʾ��/�����ܱ�");
        catMap.put("50005700", "Ʒ���ֱ�/�����ֱ�");
        catMap.put("1625", "Ůʿ����/��ʿ����/�Ҿӷ�");
        catMap.put("50011972", "Ӱ������");
        catMap.put("50011949", "�ؼ۾Ƶ�/��ɫ��ջ/��Ԣ�ù�");
        catMap.put("50010404", "�������/Ƥ��/ñ��/Χ��");
        catMap.put("50008165", "ͯװ/ͯЬ/����װ");
        catMap.put("50008090", "3C��������г�");
        catMap.put("50014812", "��Ƭ/ϴ��/ι��/�Ƴ���");
        catMap.put("50016891", "���δ�ֱ�г�����Ŀ");
        catMap.put("50014442", "��ͨƱ");
        catMap.put("50017652", "TP�����̴���");
        catMap.put("50022517", "�и�װ/�в�����Ʒ/Ӫ��");
        catMap.put("50020579", "����/�繤");
        catMap.put("50020611", "��ҵ/�칫�Ҿ�");
        catMap.put("50024971", "�³�/���ֳ�");
        catMap.put("25", "���/ģ��/����/���/����");
        catMap.put("27", "��װ����");
        catMap.put("50002768", "���˻���/����/��Ħ����");
        catMap.put("50004958", "�ƶ�/��ͨ/���ų�ֵ����");
        catMap.put("50011740", "������Ь");
        catMap.put("50007218", "�칫�豸/�Ĳ�/��ط���");
        catMap.put("50012029", "�˶�Ьnew");
        catMap.put("1801", "���ݻ���/����/����");
        catMap.put("50007216", "�ʻ��ٵ�/���ܷ���/��ֲ԰��");
        catMap.put("50011665", "����װ��/��Ϸ��/�ʺ�/����");
        catMap.put("50016422", "����/�߹�/ˮ��/��ʳ");
        catMap.put("50013864", "��Ʒ/��������/ʱ����Ʒ��");
        catMap.put("50014811", "����/�������/���");
        catMap.put("50020275", "��ͳ�̲�Ʒ/��������Ӫ��Ʒ");
        catMap.put("50025968", "˾��������Ʒר��");
        catMap.put("50026316", "��/��/����");
        catMap.put("50023724", "����");
        catMap.put("50024449", "�Ի�����");
        catMap.put("50023575", "����/�ⷿ/�·�/���ַ�/ί�з���");
        catMap.put("50025110", "��Ӱ/�ݳ�/��������");
        catMap.put("50024451", "����/����/���ͷ���");
        catMap.put("50023878", "��������ת��");
        catMap.put("50002766", "��ʳ/���/��Ҷ/�ز�");
        catMap.put("50006842", "���Ƥ��/����Ů��/�а�");
        catMap.put("50006843", "ŮЬ");
        catMap.put("50012100", "�������");
        catMap.put("50012082", "��������");
        catMap.put("50012164", "���濨/U��/�洢/�ƶ�Ӳ��");
        catMap.put("50010728", "�˶�/�٤/����/������Ʒ");
        catMap.put("50017908", "��Ʊ");
        catMap.put("50018264", "�����豸/�������");
        catMap.put("50017300", "����/����/����/���");
        catMap.put("50020485", "���/����");
        catMap.put("50025707", "������Ʊ/�ȼ���·/���η���");
        catMap.put("50025618", "����");
        catMap.put("50023804", "�Ҿӷ���");

    }

}
