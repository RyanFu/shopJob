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

public class DianyinPUV_Refer {

	public static final char CTRL_A = (char) 0x01;
	public static final  String piao="piao";
	public static final  String ticket="ticket";
	public static final String dianyin="dianying";
	public static final String y_piao="y.piao";
	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		 //String input = "C:\\Users\\longjia.zt\\Documents\\20120827"; // 文件路径
		 String mainName = "电影票"; // 报表名称
	 	  String input = args[0]; // 文件路径
		if (!new File(input).exists()) {
			System.out.println("File Not Exist ! => " + input);
			return;
		}
		
		Report report = Report.newReport(mainName + "垂直项目数据统计");

		
		Table pv = report.newGroupTable("淘宝电影总PV","淘宝电影总PV统计", "纬度", "数量");
		
		Table uv = report.newGroupTable("淘宝电影总UV","淘宝电影总UV统计", "纬度", "数量");
		
		Table dianyinrefer=report.newViewTable("dianyinrefer", mainName+ "dianyin.taobao.com来源前25");
		dianyinrefer.addCol("来源二级域名").addCol("PV数量").addCol("UV数量").addCol(Report.BREAK_VALUE);
		
		
		Table piaorefer=report.newViewTable("piaorefer", mainName+ "piao.taobao.com来源前25");
		piaorefer.addCol("来源二级域名").addCol("PV数量").addCol("UV数量").addCol(Report.BREAK_VALUE);
		
		Table y_piaorefer=report.newViewTable("y_piaorefer", mainName+ "y.piao.taobao.com来源前25");
		y_piaorefer.addCol("来源二级域名").addCol("PV数量").addCol("UV数量").addCol(Report.BREAK_VALUE);
		
		Table ticketrefer=report.newViewTable("ticketrefer", mainName+"ticket.taobao.com来源前25");
		ticketrefer.addCol("来源二级域名").addCol("PV数量").addCol("UV数量").addCol(Report.BREAK_VALUE);
		
		
		List<String> lines = Utils.readWithCharset(input, "utf-8");
		if (lines != null && lines.size() > 0) {
			for (String line : lines) {
				String[] _cols = StringUtils.splitPreserveAllTokens(line,CTRL_A);
				if ((piao+"pvuv").equals(_cols[0])) { 				
					uv.addCol(Report.newValue("piao.taobao.com UV:", _cols[2]));
					pv.addCol(Report.newValue("piao.taobao.com PV:", _cols[1]));
			
				} 
				else if ((y_piao+"pvuv").equals(_cols[0])) { 				
					uv.addCol(Report.newValue("y.piao.taobao.com UV:", _cols[2]));
					pv.addCol(Report.newValue("y.piao.taobao.com PV:", _cols[1]));
			
				} 
				else if ((dianyin+"pvuv").equals(_cols[0])) { 				
					uv.addCol(Report.newValue("dianyin.taobao.com UV:", _cols[2]));
					pv.addCol(Report.newValue("dianyin.taobao.com PV:", _cols[1]));
		
				} 
				else if ((ticket+"pvuv").equals(_cols[0])) { 				
					uv.addCol(Report.newValue("ticket.taobao.com UV:", _cols[2]));
					pv.addCol(Report.newValue("ticket.taobao.com PV:", _cols[1]));
				}
				else if ((piao+"_refer").equals(_cols[0])) { 
					if(_cols[1]==null||_cols[1]==""||_cols[1].length()==0){
						_cols[1]="无来源";
					}
					piaorefer.addCol(_cols[1]);
					piaorefer.addCol(_cols[1]+"pv", _cols[2]);
					piaorefer.addCol(_cols[1]+"uv", _cols[3]);
					piaorefer.breakRow();
				} 
				else if ((y_piao+"_refer").equals(_cols[0])) { 
					if(_cols[1]==null||_cols[1]==""||_cols[1].length()==0){
						_cols[1]="无来源";
					}
					y_piaorefer.addCol(_cols[1]);
					y_piaorefer.addCol(_cols[1]+"pv", _cols[2]);
					y_piaorefer.addCol(_cols[1]+"uv", _cols[3]);
					y_piaorefer.breakRow();
				} 
				else if ((dianyin+"_refer").equals(_cols[0])) { 
					if(_cols[1]==null||_cols[1]==""||_cols[1].length()==0){
						_cols[1]="无来源";
					}
					dianyinrefer.addCol(_cols[1]);
					dianyinrefer.addCol(_cols[1]+"pv", _cols[2]);
					dianyinrefer.addCol(_cols[1]+"uv", _cols[3]);
					dianyinrefer.breakRow();
				} 
				else if ((ticket+"_refer").equals(_cols[0])) { 
					if(_cols[1]==null||_cols[1]==""||_cols[1].length()==0){
						_cols[1]="无来源";
					}
					ticketrefer.addCol(_cols[1]);
					ticketrefer.addCol(_cols[1]+"pv", _cols[2]);
					ticketrefer.addCol(_cols[1]+"uv", _cols[3]);
					ticketrefer.breakRow();
				}
				
			}//for			
			
		}//if
		XmlReportFactory.dump(report, new FileOutputStream(input + ".xml"));

	}

}
