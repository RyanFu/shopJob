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
 * @description: 本地生活电子券，数据统计报表
 */
public class KaBaoReport {
	
	public static final char CTRL_A=(char)0x01; //分隔符
	
	private static String reportName = "本地生活电子券数据统计"; 
	
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
		
		String tableId1 = "kabao_refer_pv";
		Table refer = report.newGroupTable(tableId1, "lifekabao refer 统计", "refer","pv");
		
		String tableId2 = "kabao_city_pv";
		Table city = report.newGroupTable(tableId2, "lifekabao 城市 统计", "city","pv");
		
		
		for(String line : lines){
			String[] _cols = StringUtils.splitPreserveAllTokens(line,CTRL_A);//取出是哪个表
			
			if( tableId1.equals(_cols[0]) ){	
				
				if(_cols[1] == null || "".equals(_cols[1].trim()) ){//插入refer前10
					refer.addCol("其他", _cols[2]);
				}else{
					refer.addCol(_cols[1], _cols[2]);
				}
			}
			
			if( tableId2.equals(_cols[0]) ){	
				
				if(_cols[1] == null || "".equals(_cols[1].trim())){//插入city前10
					city.addCol("其他", _cols[2]);
				}else{
					city.addCol(_cols[1], _cols[2]);
				}
			}
			
			
		}// end for
		
	}

}
