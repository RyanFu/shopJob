/**
 * 
 */
package org.dueam.hadoop.bp.report.fang;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

/**
 * @author longjia.zt
 * 
 */
public class FangChuiZhi {
	public static final char CTRL_A = (char) 0x01;;
	public static final String ONEDAYBUILDING = "oneDayBuilding"; // ͷ����
	public static final String ALLBUILDING = "allBuilding"; // ͷ����
	public static final String LOUPANITEMCOUNT = "loupanItemcount"; // ͷ����
	public static final String LOUPANITEMCOUNTONLINE = "loupanItemcountonline"; // ͷ����
	public static final String LOUPANITEMCOUNTOFFLINE = "loupanItemcountoffline"; // ͷ����
	public static final String LOUPANCITY = "loupanCity"; // ͷ����
	
	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		//String input = "C:\\Users\\longjia.zt\\Documents\\20120605"; // �ļ�·��
		String mainName = "�Ա�����"; // ��������
	 	 String input = args[0]; // �ļ�·��
		if (!new File(input).exists()) {
			System.out.println("File Not Exist ! => " + input);
			return;
		}

		Report report = Report.newReport(mainName + "��ֱ��Ŀ����ͳ��");
		
		Table oneDayBuilding = report.newGroupTable("oneDayBuilding",
				"����¥������", "γ��", "����");

		Table allBuilding = report.newGroupTable("allBuilding",
				"��¥������", "γ��", "����");
		
		Table loupanCity=report.newViewTable("loupanCity", mainName
				+ "�ֳ���¥������ǰ20����");
		loupanCity.addCol("ά��").addCol("����")
		.addCol(Report.BREAK_VALUE);
		
		
		Table loupanItemcount=report.newViewTable("loupanItemcount", mainName
				+ "¥������ҿ��ı���������û�б�ɾ����");
		loupanItemcount.addCol("¥������").addCol("�ҿ���������")
		.addCol(Report.BREAK_VALUE);
		
		Table loupanItemcountonline=report.newViewTable("loupanItemcountonline", mainName
				+ "¥������ҿ������߱�������");
		loupanItemcountonline.addCol("¥������").addCol("�ҿ����߱�������")
		.addCol(Report.BREAK_VALUE);
		
		
		Table loupanItemcountoffline=report.newViewTable("loupanItemcountoffline", mainName
				+ "¥������ҿ����¼ܱ�������");
		loupanItemcountoffline.addCol("¥������").addCol("�ҿ��¼ܱ�������")
		.addCol(Report.BREAK_VALUE);
		
		
		List<String> lines = Utils.readWithCharset(input, "utf-8");
		if (lines != null && lines.size() > 0) {
			int allB=0;
			int allitems=0;
			int allBonline=0;
			int allitemsonline=0;
			int allBoffline=0;
			int allitemsoffline=0;
			for (String line : lines) {
				String[] _cols = StringUtils.splitPreserveAllTokens(line,
						CTRL_A);
				if (ONEDAYBUILDING.equals(_cols[0])) {// �·�����					
					oneDayBuilding.addCol(Report.newValue("ÿ������¥��", _cols[1]));
					
				} else if (ALLBUILDING.equals(_cols[0])) {// �·�����
					allBuilding.addCol(Report.newValue("����¥������", _cols[1]));
	
					
				} else if (LOUPANITEMCOUNT.equals(_cols[0])) {// �·�����
					loupanItemcount.addCol(_cols[1]);
					loupanItemcount.addCol(_cols[1]+"NUM", _cols[2]);
					loupanItemcount.breakRow();
					allB++;
					allitems=allitems+Integer.parseInt(_cols[2]);
				}
				
			 else if (LOUPANITEMCOUNTONLINE.equals(_cols[0])) {// �·�����
				 loupanItemcountonline.addCol(_cols[1]);
				 loupanItemcountonline.addCol(_cols[1]+"NUM", _cols[2]);
				 loupanItemcountonline.breakRow();
				 allBonline++;
				allitemsonline=allitemsonline+Integer.parseInt(_cols[2]);
			
			
		} else if (LOUPANITEMCOUNTOFFLINE.equals(_cols[0])) {// �·�����
			loupanItemcountoffline.addCol(_cols[1]);
			loupanItemcountoffline.addCol(_cols[1]+"NUM", _cols[2]);
			loupanItemcountoffline.breakRow();
			allBoffline++;
			allitemsoffline=allitemsoffline+Integer.parseInt(_cols[2]);
		}
		 else if (LOUPANCITY.equals(_cols[0])) {// �·�����
			 loupanCity.addCol(_cols[1]);
			 loupanCity.addCol(_cols[1]+"NUM", _cols[2]);
			 loupanCity.breakRow();
			}
				
				
			}//for
			loupanItemcount.addCol("¥������NUM","¥��������"+String.valueOf(allB));
			loupanItemcount.addCol("��Ʒ����GMV","�ҿ���Ʒ������"+String.valueOf(allitems));
			loupanItemcount.breakRow();
			
			loupanItemcountonline.addCol("¥������NUM","¥��������"+String.valueOf(allBonline));
			loupanItemcountonline.addCol("��Ʒ����GMV","�ҿ�������Ʒ������"+String.valueOf(allitemsonline));
			loupanItemcountonline.breakRow();
			
			
			loupanItemcountoffline.addCol("¥������NUM","¥��������"+String.valueOf(allBoffline));
			loupanItemcountoffline.addCol("��Ʒ����GMV","�ҿ��¼���Ʒ������"+String.valueOf(allitemsoffline));
			loupanItemcountoffline.breakRow();
			
		}//if
		XmlReportFactory.dump(report, new FileOutputStream(input + ".xml"));


	}

}
