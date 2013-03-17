package org.dueam.hadoop.bp.report;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

/**
 * �߼�����������ͳ�Ʊ���
 */
public class AdSearchReport {
	 private static char CTRL_A = (char) 0x01;
	    public static void main(String[] args) throws IOException {
	        String input = args[0];
	        if (!new File(input).exists()) {
	            System.out.println("File Not Exist ! => " + input);
	            return;
	        }
	        Report report = Report.newReport("�߼�������ͳ�Ʊ���");
	        Table totalTable = report.newTable("total", "�������ݻ���");
	        Table filterFormTable = report.newTable("filterForm", "ɸѡ���ʹ�����");
	        Table sellerTable = report.newTable("seller", "�����ҵ�ʹ�����");
	        Table referPvTable = report.newTable("referPv", "�߼���������Դpvͳ��");
	        Table referUvTable = report.newTable("referUv", "�߼���������Դuvͳ��");
//			Table oneWeek = report.newTable("oneWeek", "һ�����û��ķ���Ƶ��ͳ��");

	        int sellerPv = 0;
	        int nickSellerPv = 0;
	        int keywordModule = 0;
	        int minCount=2;

	        for (String line : Utils.readWithCharset(input, "utf-8")) {
	            String[] _allCols = StringUtils.splitPreserveAllTokens(line, CTRL_A);

	            if (_allCols.length > 1) {
	                if (StringUtils.equals(_allCols[0], "entry")) {
	                	//�������
	                	totalTable.addCol("����pv", _allCols[1]);
	                	totalTable.addCol("����uv", _allCols[2]);
	                }else if(StringUtils.equals(_allCols[0], "noresult")){
	                	//����list�޽�������
	                	totalTable.addCol("��ת��list�޽����pv", _allCols[1]);
	                }else if (StringUtils.equals(_allCols[0], "filterForm")) {
	                	//������������
	                	totalTable.addCol("���������pv",_allCols[1]);
	                	totalTable.addCol("���������uv",_allCols[2]);
	                	//ɸѡ������ʹ�����
	                	filterFormTable.addCol("�г�",_allCols[3]);
	                	filterFormTable.addCol("�ؼ���",_allCols[4]);
	                	filterFormTable.addCol("�ǳ�",_allCols[5]);
	                	filterFormTable.addCol("��������",_allCols[6]);
	                	filterFormTable.addCol("�۸���������",_allCols[7]);
	                	filterFormTable.addCol("�۸���������",_allCols[8]);
	                	filterFormTable.addCol("�������õȼ�",_allCols[9]);
	                	filterFormTable.addCol("�����߱���",_allCols[10]);
	                	filterFormTable.addCol("��Ʒ����",_allCols[11]);
	                	filterFormTable.addCol("Ʒ����Ȩ",_allCols[12]);
	                	filterFormTable.addCol("�����˻�",_allCols[13]);
	                	filterFormTable.addCol("��һ����",_allCols[14]);
	                	filterFormTable.addCol("�������ʼ�",_allCols[15]);
	                	filterFormTable.addCol("��Ʒ�Ż�",_allCols[16]);
	                	filterFormTable.addCol("����",_allCols[17]);
	                	filterFormTable.addCol("�˷���",_allCols[18]);
	                	filterFormTable.addCol("24Сʱ����",_allCols[19]);
	                	filterFormTable.addCol("���緢��",_allCols[20]);
	                	filterFormTable.addCol("����",_allCols[21]);
	                	filterFormTable.addCol("ȫ��",_allCols[22]);
	                	filterFormTable.addCol("��������",_allCols[23]);
	                	filterFormTable.addCol("���ÿ�",_allCols[24]);
	                	filterFormTable.addCol("���汦��",_allCols[25]);
	                	filterFormTable.addCol("������Ʒ",_allCols[26]);
	                }else if (StringUtils.equals(_allCols[0], "keyword")) {
	                	if(StringUtils.equals("1", _allCols[1])){
	                		//�ؼ��ʷ���ģ��֮����Ʒ��
	                		totalTable.addCol("����Ʒ�Ƶķ���pv", _allCols[2]);
	                		totalTable.addCol("����Ʒ�Ƶķ���uv", _allCols[3]);
	                		keywordModule += Integer.parseInt(_allCols[2]);
	                	}else if(StringUtils.equals("2", _allCols[1])){
	                		//�ؼ��ʷ���ģ��֮������Ʒ
	                		totalTable.addCol("������Ʒ�ķ���pv", _allCols[2]);
	                		totalTable.addCol("������Ʒ�ķ���uv", _allCols[3]);
	                		//�ؼ��ʷ���ģ�����pv
	                		keywordModule += Integer.parseInt(_allCols[2]);
	                		totalTable.addCol("�ؼ��ʷ���ģ����ܷ���pv", new Integer(keywordModule).toString());
	                	}
	                }else if (StringUtils.equals(_allCols[0], "survey")) {
	                	//�ʾ�����ʹ�����
	                    totalTable.addCol("�ʾ�����pv", _allCols[1]);
	                    totalTable.addCol("�ʾ�����uv", _allCols[2]);
	                }else if(StringUtils.equals(_allCols[0], "all")){
	                	//���ʸ߼����������ҡ�������
	                    if(StringUtils.equals(_allCols[1], "seller")){
	                    	sellerTable.addCol("���ʸ߼�������������", _allCols[2]);
	                    	sellerPv=Integer.parseInt(_allCols[2]);
	                    }else if(StringUtils.equals(_allCols[1], "seller_and_buyer")){
	                    	int buyerPv = Integer.parseInt(_allCols[2]) - sellerPv;
	                    	sellerTable.addCol("���ʸ߼������������", new Integer(buyerPv).toString());
	                    }
	                }else if(StringUtils.equals(_allCols[0], "nick")){
	                	//�ǳ����������ҡ�������
	                	if(StringUtils.equals(_allCols[1], "seller")){
	                		sellerTable.addCol("�ǳ�������������", _allCols[2]);
	                		nickSellerPv = Integer.parseInt(_allCols[2]);
	                	}else if(StringUtils.equals(_allCols[1], "seller_and_buyer")){
	                		int nickBuyerPv = Integer.parseInt(_allCols[2]) - nickSellerPv;
	                		sellerTable.addCol("�ǳ������������", new Integer(nickBuyerPv).toString());
	                	}
	                }else if(StringUtils.equals(_allCols[0],"refer")){
	                	//�߼�������Դͳ��
	                	if(NumberUtils.toLong(_allCols[2])>minCount){
	                		referPvTable.addCol(_allCols[1],_allCols[2]);
	                	}
	                	if(NumberUtils.toLong(_allCols[3])>minCount){
	                		referUvTable.addCol(_allCols[1], _allCols[3]);
	                	}
	                }
	            }
	        }
	        filterFormTable.sort(Table.SORT_VALUE);
	        referPvTable.sort(Table.SORT_VALUE);
	        referUvTable.sort(Table.SORT_VALUE);
	        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
	    }
}
