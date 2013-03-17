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
 * @author:longlong.ckx
 * @email:longlong@taobao.com
 * @date:2012-12-05
 * @description: 本地生活电子券access，数据统计报表
 */
public class KaBaoAccessReport {
	
	public static final char CTRL_A=(char)0x01; //分隔符
	
	private static String reportName = "本地生活电子券access数据统计"; 
	
	// main 
	public static void main(String[] args) throws IOException {
		String input = args[0];
		//String input = "d:\\20130121";
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
	
	// 点评数据统计报表
	public static void generateTableReviewStatistics(Report report ,List<String> lines){
		
		String tableId = "kabao_access";
		Table refer = report.newGroupTable(tableId, "lifekabao 访问 统计", "类别","pv");
		
		
		for(String line : lines){
			String[] _cols = StringUtils.splitPreserveAllTokens(line,"\t");//取出是哪个表
			
			refer.addCol(_cols[0], _cols[1]);
			
		}// end for
		
	}

}
