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

public class FangPvCount {

	public static final char CTRL_A = (char) 0x01;;
	public static final String LIST= "list"; // ͷ����
	public static final String BUILDING_DETAIL = "buildingDetail"; // ͷ����
	public static final String ITEM_DETAIL = "itemDetail"; // ͷ����
	public static final String LIST_IN = "listIn"; // ͷ����
	public static final String BUILDING_DETAIL_IN = "buildingDetailIn"; // ͷ����
	public static final String ITEM_DETAIL_IN = "itemDetailIn"; // ͷ����
	
	
	public static final String LIST_PAGE = "http://house.taobao.com/building/list.htm"; // �����ų���ҳ
	public static final String BUILDING_DETAIL_PAGE = "http://house.taobao.com/building/detail.htm"; // �����ų���ҳ
	public static final String ITEM_DETAIL_PAGE = "http://house.taobao.com/item/item.htm"; // �����ų���ҳ
	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
//		String input = "D:\\20120705"; // �ļ�·��
		String mainName = "�Ա�����"; // ��������
	 	String input = args[0]; // �ļ�·��
		if (!new File(input).exists()) {
			System.out.println("File Not Exist ! => " + input);
			return;
		}

		Report report = Report.newReport(mainName + "��ֱ��Ŀҳ���������ͳ��");
		
		Table pv_uv = report.newGroupTable("Pv_Uv",
				"ҳ�����ͳ��", "γ��", "����");
		
		Table listVisit = report.newViewTable("listVisit", mainName
				+ "¥��list UV��Դ");
		listVisit.addCol("ά��").addCol("����").addCol(Report.BREAK_VALUE);
		
		Table listDetailVisit = report.newViewTable("listDetailVisit", mainName
				+ "¥��detail UV��Դ");
		listDetailVisit.addCol("ά��").addCol("����").addCol(Report.BREAK_VALUE);
		
		Table itemDetailVisit = report.newViewTable("itemDetailVisit", mainName
				+ "����detail UV��Դ");
		itemDetailVisit.addCol("ά��").addCol("����").addCol(Report.BREAK_VALUE);
		
		List<String> lines = Utils.readWithCharset(input, "utf-8");
		if (lines != null && lines.size() > 0) {
			for (String line : lines) {
				String[] _cols = StringUtils.splitPreserveAllTokens(line,
						CTRL_A);
				if (LIST.equals(_cols[0])) {// �·�����					
					pv_uv.addCol(Report.newValue("list PV", _cols[1]));
					pv_uv.addCol(Report.newValue("list UV", _cols[2]));
					
				} else if (BUILDING_DETAIL.equals(_cols[0])) {// �·�����
					pv_uv.addCol(Report.newValue("¥��detail PV", _cols[1]));
					pv_uv.addCol(Report.newValue("¥��detail UV", _cols[2]));
					
				} else if (ITEM_DETAIL.equals(_cols[0])) {// �·�����
					pv_uv.addCol(Report.newValue("����detail PV", _cols[1]));
					pv_uv.addCol(Report.newValue("����detail UV", _cols[2]));
				}else if(LIST_IN.equals(_cols[0])){
					if(LIST_PAGE.equals(_cols[1])){
						listVisit.addCol("��ҳ�����");
						listVisit.addCol(_cols[2]);
						listVisit.breakRow();
					}else if(_cols[1].isEmpty()){
						listVisit.addCol("δ֪��Դ��");
						listVisit.addCol(_cols[2]);
						listVisit.breakRow();
					}else{
						listVisit.addCol(_cols[1]);
						listVisit.addCol(_cols[2]);
						listVisit.breakRow();
					}
				}else if(BUILDING_DETAIL_IN.equals(_cols[0])){
					if(BUILDING_DETAIL_PAGE.equals(_cols[1])){
						listDetailVisit.addCol("��ҳ�����");
						listDetailVisit.addCol(_cols[2]);
						listDetailVisit.breakRow();
					}else if(_cols[1].isEmpty()){
						listDetailVisit.addCol("δ֪��Դ��");
						listDetailVisit.addCol(_cols[2]);
						listDetailVisit.breakRow();
					}else{
						listDetailVisit.addCol(_cols[1]);
						listDetailVisit.addCol(_cols[2]);
						listDetailVisit.breakRow();
					}
					
				}else if(ITEM_DETAIL_IN.equals(_cols[0])){
					if(ITEM_DETAIL_PAGE.equals(_cols[1])){
						itemDetailVisit.addCol("��ҳ�����");
						itemDetailVisit.addCol(_cols[2]);
						itemDetailVisit.breakRow();
					}else if(_cols[1].isEmpty()){
						itemDetailVisit.addCol("δ֪��Դ��");
						itemDetailVisit.addCol(_cols[2]);
						itemDetailVisit.breakRow();
					}else{
						itemDetailVisit.addCol(_cols[1]);
						itemDetailVisit.addCol(_cols[2]);
						itemDetailVisit.breakRow();
					}
				}
			}
		}
				
		XmlReportFactory.dump(report, new FileOutputStream(input + ".xml"));

	}

}
