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
public class smallAndBigOrder {
	public static final char CTRL_A = (char) 0x01;;
	public static final String SMALL = "small"; // ͷ����
	public static final String BIG = "big"; // ͷ����
	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		 //String input = "C:\\Users\\longjia.zt\\Documents\\20120517"; // �ļ�·��
		String mainName = "�Ա�����"; // ��������
	   String input = args[0]; // �ļ�·��
		if (!new File(input).exists()) {
			System.out.println("File Not Exist ! => " + input);
			return;
		}
		Report report = Report.newReport(mainName + "�����ݡ���С��ҵ��֧����");

		
		
		Table oneWeeksmallNewPos = report.newViewTable("oneWeeksmallNewPos", mainName
				+ "��POSС��������");
		oneWeeksmallNewPos.addCol("�����û���").addCol("�ɽ�����").addCol("֧�����ɽ����")
				.addCol(Report.BREAK_VALUE);
		

		
		Table oneWeekbigNewPos = report.newViewTable("oneWeekbigNewPos", mainName
				+ "��POS��������");
		oneWeekbigNewPos.addCol("�����û���").addCol("�ɽ�����").addCol("֧�����ɽ����")
				.addCol(Report.BREAK_VALUE);
		
		List<String> lines = Utils.readWithCharset(input, "utf-8");
		if (lines != null && lines.size() > 0) {
			float smallOrderFee1 = 0.00f;
			int smallDoneCount1= 0;
			float bigOrderFee1= 0.00f;
			int bigDoneCount1= 0;
			
			for (String line : lines) {
				String[] _cols = StringUtils.splitPreserveAllTokens(line,
						CTRL_A);
				if(_cols[1].contains("����") ||_cols[1].contains("����")){
					continue;
				}
			 if ("oneWeeksmallNewPos".equals(_cols[0])) {// �·�����
					oneWeeksmallNewPos.addCol(_cols[1]);
					oneWeeksmallNewPos.addCol(_cols[1]+"NUM", _cols[2]);
					oneWeeksmallNewPos.addCol(_cols[1]+"GMV", _cols[3]);
					oneWeeksmallNewPos.breakRow();
					smallDoneCount1 = smallDoneCount1+Integer.valueOf(_cols[2]);
					smallOrderFee1 = smallOrderFee1 + Float.parseFloat(_cols[3]);
			
				} 
				else if ("oneWeekbigNewPos".equals(_cols[0])) {// �·�����
					oneWeekbigNewPos.addCol(_cols[1]);
					oneWeekbigNewPos.addCol(_cols[1]+"NUM", _cols[2]);
					oneWeekbigNewPos.addCol(_cols[1]+"GMV", _cols[3]);
					oneWeekbigNewPos.breakRow();
					bigDoneCount1 = bigDoneCount1+Integer.valueOf(_cols[2]);
					bigOrderFee1 = bigOrderFee1 + Float.parseFloat(_cols[3]);
			
				} 
		

			}//for
			
			
			oneWeeksmallNewPos.addCol("��POSС���ɽ��ϼ�");
			oneWeeksmallNewPos.addCol("��POSС���ɽ��ϼ�NUM",String.valueOf(smallDoneCount1));
			oneWeeksmallNewPos.addCol("��POSС���ɽ��ϼ�GMV",String.valueOf(smallOrderFee1));
			oneWeeksmallNewPos.breakRow();
			
			oneWeekbigNewPos.addCol("��POS�󶨳ɽ��ϼ�");
			oneWeekbigNewPos.addCol("��POS�󶨳ɽ��ϼ�NUM",String.valueOf(bigDoneCount1));
			oneWeekbigNewPos.addCol("��POS�󶨳ɽ��ϼ�GMV",String.valueOf(bigOrderFee1));
			oneWeekbigNewPos.breakRow();


		}//if
		XmlReportFactory.dump(report, new FileOutputStream(input + ".xml"));


	}

}
