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
public class FangMoneySomething {
	public static final char CTRL_A = (char) 0x01;;
	public static final String NEW_HOUSE = "newHouse"; // ͷ����
	public static final String ER_SHOU_HOUSE = "ershouHouse"; // ͷ����
	public static final String ALL_HOUSE = "allHouse"; // ͷ����
	public static final String CHUIZHI = "chuizhi"; // ͷ����
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

		Report report = Report.newReport(mainName + "�����ݡ��·���Ŀ-�ⷿ��Ŀ-����Ŀ��");
		Table newHouse=report.newViewTable("smallOrder", mainName
				+ "�·���Ŀ����");
		newHouse.addCol("γ��").addCol("����")
		.addCol(Report.BREAK_VALUE);
		
		Table chuizhi=report.newViewTable("chuizhi", mainName
				+ "��ֱ���·���Ŀ����");
		chuizhi.addCol("γ��").addCol("����")
		.addCol(Report.BREAK_VALUE);
		
		Table ershouHouse=report.newViewTable("ershouHouse", mainName
				+ "�ⷿ��Ŀ��Ŀ����");
		ershouHouse.addCol("γ��").addCol("����")
		.addCol(Report.BREAK_VALUE);

		
		Table allHouse=report.newViewTable("allHouse", mainName
				+ "����Ŀ��Ŀ����");
		allHouse.addCol("γ��").addCol("����")
		.addCol(Report.BREAK_VALUE);
		
		List<String> lines = Utils.readWithCharset(input, "utf-8");
		if (lines != null && lines.size() > 0) {

			for (String line : lines) {
				String[] _cols = StringUtils.splitPreserveAllTokens(line,
						CTRL_A);
				if (NEW_HOUSE.equals(_cols[0])) {// �·�����					
					newHouse.addCol(_cols[1]);
					newHouse.addCol(_cols[1]+"NUM", _cols[2]);
					newHouse.breakRow();
					
				} else if (ER_SHOU_HOUSE.equals(_cols[0])) {// �·�����
					ershouHouse.addCol(_cols[1]);
					ershouHouse.addCol(_cols[1]+"NUM", _cols[2]);
					ershouHouse.breakRow();
					
				} else if (ALL_HOUSE.equals(_cols[0])) {// �·�����
					allHouse.addCol(_cols[1]);
					allHouse.addCol(_cols[1]+"NUM", _cols[2]);
					allHouse.breakRow();
				}
				 else if (CHUIZHI.equals(_cols[0])) {// �·�����
					 chuizhi.addCol(_cols[1]);
					 chuizhi.addCol(_cols[1]+"NUM", _cols[2]);
					 chuizhi.breakRow();
					}

			}//for

		}//if
		XmlReportFactory.dump(report, new FileOutputStream(input + ".xml"));


	}

}
