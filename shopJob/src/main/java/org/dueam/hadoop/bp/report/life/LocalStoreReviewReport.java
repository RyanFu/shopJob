package org.dueam.hadoop.bp.report.life;

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
 * @author:yuanbo.zk
 * @email:yuanbo.zk@taobao.com
 * @date:2012-12-05
 * @description: �����̻�����������ͳ�Ʊ���
 */
public class LocalStoreReviewReport {
	
	public static final char CTRL_A=(char)0x01; //�ָ���
	
	private static String reportName = "�����̻���������ͳ��"; 
	
	// main 
	public static void main(String[] args) throws IOException {
		String input = args[0];
		Report report = generateReport(input,reportName);
		XmlReportFactory.dump(report, new FileOutputStream(input + ".xml"));
	}
	
	// generate my report here.
	public static Report generateReport(String input,String reportName) throws IOException{
		Report report = Report.newReport(reportName);

		if (!new File(input).exists()) {
			throw new IOException("File Not Exist !" + input);
		}
		List<String> lines = Utils.readWithCharset(input, "utf-8");
		if (lines!= null && lines.size() > 0) {	
			// generate my tables here
			generateTablesInReport(report,lines);
		}
		return report;
	}
	
	// generate my tables
	public static void generateTablesInReport(Report report ,List<String> lines){
		generateTableReviewStatistics(report,lines);
	}
	
	// ��������ͳ�Ʊ���
	public static void generateTableReviewStatistics(Report report ,List<String> lines){
		String tableId = "review_all";
		Table review = report.newGroupTable(tableId, "�����̻���������ͳ��", "ά��", "����");
		
		long review_count = 0;// ��������
		long review_store_count = 0;// ������������
		long review_user_count = 0;// ��������û���
		long review_city_count = 0;// �������������
		
		for(String line : lines){
			String[] _cols = StringUtils.splitPreserveAllTokens(line,CTRL_A);
			if( tableId.equals(_cols[0]) ){
				review_count += Long.parseLong(_cols[1]);
				review_store_count += Long.parseLong(_cols[2]);
				review_user_count += Long.parseLong(_cols[3]);
				review_city_count += Long.parseLong(_cols[4]);
			}
			
		}// end for
		
		review.addCol("��������", String.valueOf(review_count));
		review.addCol("�������̻���", String.valueOf(review_store_count));
		review.addCol("��������û���", String.valueOf(review_user_count));
		review.addCol("�������������", String.valueOf(review_city_count));
	}

}
