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
public class AppointMentOneWeek {

	public static final char CTRL_A = (char) 0x01;;
	public static final String NEW = "new"; // ͷ����
	public static final String ERSHOU = "ershou"; // ͷ����
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
		Report report = Report.newReport(mainName + "�����ݡ��·� ���ַ�-ԤԼ���ݡ�");
		Table newHouseAppointTable = report.newViewTable("newHouseAppoint", mainName
				+ "�·�ԤԼ������");
		newHouseAppointTable.addCol("�����û���").addCol("ԤԼ����")
				.addCol(Report.BREAK_VALUE);
		
		
		Table ershouHouseAppointTable = report.newViewTable("ershouHouseAppoint", mainName
				+ "�ⷿԤԼ����");
		ershouHouseAppointTable.addCol("�����û���").addCol("ԤԼ����")
				.addCol(Report.BREAK_VALUE);
		
		List<String> lines = Utils.readWithCharset(input, "utf-8");
		if (lines != null && lines.size() > 0) {
			int newHouseAppointCount= 0;
			int ershouHouseAppointCount= 0;
			for (String line : lines) {
				String[] _cols = StringUtils.splitPreserveAllTokens(line,
						CTRL_A);
				if(_cols[1].contains("����") ||_cols[1].contains("����")){
					continue;
				}
				if (NEW.equals(_cols[0])) {// �·�����
					newHouseAppointTable.addCol(_cols[1]);
					newHouseAppointTable.addCol(_cols[1]+"NUM", _cols[2]);
					newHouseAppointTable.breakRow();
					newHouseAppointCount = newHouseAppointCount+Integer.valueOf(_cols[2]);				
					
				} else if (ERSHOU.equals(_cols[0])) {// �·�����
					ershouHouseAppointTable.addCol(_cols[1]);
					ershouHouseAppointTable.addCol(_cols[1]+"NUM", _cols[2]);
					ershouHouseAppointTable.breakRow();
					ershouHouseAppointCount = ershouHouseAppointCount+Integer.valueOf(_cols[2]);				
				
				} 

			}//for
			
			newHouseAppointTable.addCol("�·�ԤԼ�ϼ�");
			newHouseAppointTable.addCol("�·�ԤԼ�ϼ�NUM",String.valueOf(newHouseAppointCount));
			newHouseAppointTable.breakRow();
			
			ershouHouseAppointTable.addCol("�ⷿԤԼ�ϼ�");
			ershouHouseAppointTable.addCol("�ⷿԤԼ�ϼ�NUM",String.valueOf(ershouHouseAppointCount));
			ershouHouseAppointTable.breakRow();

		}//if
		XmlReportFactory.dump(report, new FileOutputStream(input + ".xml"));


	}

}
