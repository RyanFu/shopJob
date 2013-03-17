package org.dueam.hadoop.bp.report;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.dueam.hadoop.common.util.Fmt;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

/**
 * ������������ͳ��
 */
public class Mysize {
    private static char TAB = (char) 0x09;
    static Map<String, String> typeMap = Utils.asLinkedMap("jac", "�ų�", "jak", "�ſ�", "sxw","����Χ","xxw", "����Χ", "xw",
    		"��Χ", "yw","��Χ","tw", "��Χ","bc","�۳�","jk", "���", "tc", "�ȳ�","sg","���","tz","����","dtw","����Χ");

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

        Report report = Report.newReport("������������ͳ��");

        Table role = report.newGroupTable("roleNumber", "�û�������ɫ����ͳ��");
       // role.addCol("������ɫ��").addCol("�û���").addCol("���û���").addCol("��ռ����").addCol(Report.BREAK_VALUE);

        Table hot = report.newGroupTable("hotData", "�ȵ㲿λ����ͳ��");
       // hot.addCol("��λ����").addCol("�û���").addCol("���û���").addCol("��ռ����").addCol(Report.BREAK_VALUE);

        Table foot = report.newViewTable("footLength", "�ų����ݲο�ռ��ͳ��");
        foot.addCol("�û���").addCol("���û���").addCol("��ռ����").addCol(Report.BREAK_VALUE);

        Table sex = report.newViewTable("sex", "��ɫ�Ա�ֲ�");
        sex.addCol("����").addCol("���û���").addCol("��ռ����").addCol(Report.BREAK_VALUE);

        Table quantity = report.newViewTable("quantity", "��ռ��ͳ�ƣ�ͳ�ƴ����������û�������д�������");
        quantity.addCol("����д�û���").addCol("���û���").addCol("��ռ����").addCol(Report.BREAK_VALUE);

        for (String line : Utils.readWithCharset(input, "utf-8")) {
        	if(line == null) return;
            String[] _allCols = StringUtils.splitByWholeSeparator(line, "\t");

            String type = _allCols[0];

            if ("part".equalsIgnoreCase(type)) {

                String jac = _allCols[1];
                String jak = _allCols[2];
                String sxw = _allCols[3];
                String xxw = _allCols[4];
                String xw = _allCols[5];
                String yw = _allCols[6];
                String tw = _allCols[7];
                String bc = _allCols[8];
                String jk = _allCols[9];
                String tc = _allCols[10];
                String sg = _allCols[11];
                String tz = _allCols[12];
                String dtw = _allCols[13];

            	hot.addCol(typeMap.get("jac"),jac);
            	hot.addCol(typeMap.get("jak"),jak);
            	hot.addCol(typeMap.get("sxw"),sxw);
            	hot.addCol(typeMap.get("xxw"),xxw);
            	hot.addCol(typeMap.get("jk"),jk);
            	hot.addCol(typeMap.get("xw"),xw);
            	hot.addCol(typeMap.get("yw"),yw);
            	hot.addCol(typeMap.get("tw"),tw);
            	hot.addCol(typeMap.get("dtw"),dtw);
            	hot.addCol(typeMap.get("bc"),bc);
            	hot.addCol(typeMap.get("sg"),sg);
            	hot.addCol(typeMap.get("tz"),tz);
            	hot.addCol(typeMap.get("tc"),tc);

            }
            if("1".equalsIgnoreCase(type)||"2".equalsIgnoreCase(type)||"3".equalsIgnoreCase(type)||"4".equalsIgnoreCase(type)||"5".equalsIgnoreCase(type)){
            	role.addCol(type,_allCols[1]);  //������1����ɫ���û���
            }
            if("quanlity".equalsIgnoreCase(type)){
            	quantity.addCol(_allCols[1]).addCol(_allCols[2]).addCol(Fmt.parent2(_allCols[1], _allCols[2])).breakRow();  //���ֶδ�������ɫ��ȷʵ��д�����ݵ��û���
            }
            if("foot".equalsIgnoreCase(type)){
            	foot.addCol(_allCols[1]).addCol(_allCols[2]).addCol(Fmt.parent2(_allCols[1], _allCols[2])).breakRow();  //���ֶδ���ο����ձ���û���
            }
            if("sex".equalsIgnoreCase(type)){
            	sex.addCol(_allCols[1]).addCol(_allCols[2]).addCol(Fmt.parent2(_allCols[1], _allCols[2])).breakRow();  //���ֶδ���ο����ձ���û���
            }
        }
        role.sort(Table.SORT_KEY);
        hot.sort(Table.SORT_VALUE);
        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
    }

}
