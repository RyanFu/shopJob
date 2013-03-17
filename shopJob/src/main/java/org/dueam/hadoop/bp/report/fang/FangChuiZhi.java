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
	public static final String ONEDAYBUILDING = "oneDayBuilding"; // 头数据
	public static final String ALLBUILDING = "allBuilding"; // 头数据
	public static final String LOUPANITEMCOUNT = "loupanItemcount"; // 头数据
	public static final String LOUPANITEMCOUNTONLINE = "loupanItemcountonline"; // 头数据
	public static final String LOUPANITEMCOUNTOFFLINE = "loupanItemcountoffline"; // 头数据
	public static final String LOUPANCITY = "loupanCity"; // 头数据
	
	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		//String input = "C:\\Users\\longjia.zt\\Documents\\20120605"; // 文件路径
		String mainName = "淘宝房产"; // 报表名称
	 	 String input = args[0]; // 文件路径
		if (!new File(input).exists()) {
			System.out.println("File Not Exist ! => " + input);
			return;
		}

		Report report = Report.newReport(mainName + "垂直项目数据统计");
		
		Table oneDayBuilding = report.newGroupTable("oneDayBuilding",
				"新增楼盘数量", "纬度", "数量");

		Table allBuilding = report.newGroupTable("allBuilding",
				"总楼盘数量", "纬度", "数量");
		
		Table loupanCity=report.newViewTable("loupanCity", mainName
				+ "分城市楼盘数量前20城市");
		loupanCity.addCol("维度").addCol("数量")
		.addCol(Report.BREAK_VALUE);
		
		
		Table loupanItemcount=report.newViewTable("loupanItemcount", mainName
				+ "楼盘下面挂靠的宝贝数量【没有被删除】");
		loupanItemcount.addCol("楼盘名称").addCol("挂靠宝贝数量")
		.addCol(Report.BREAK_VALUE);
		
		Table loupanItemcountonline=report.newViewTable("loupanItemcountonline", mainName
				+ "楼盘下面挂靠的在线宝贝数量");
		loupanItemcountonline.addCol("楼盘名称").addCol("挂靠在线宝贝数量")
		.addCol(Report.BREAK_VALUE);
		
		
		Table loupanItemcountoffline=report.newViewTable("loupanItemcountoffline", mainName
				+ "楼盘下面挂靠的下架宝贝数量");
		loupanItemcountoffline.addCol("楼盘名称").addCol("挂靠下架宝贝数量")
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
				if (ONEDAYBUILDING.equals(_cols[0])) {// 新房数据					
					oneDayBuilding.addCol(Report.newValue("每天新增楼盘", _cols[1]));
					
				} else if (ALLBUILDING.equals(_cols[0])) {// 新房数据
					allBuilding.addCol(Report.newValue("所有楼盘数量", _cols[1]));
	
					
				} else if (LOUPANITEMCOUNT.equals(_cols[0])) {// 新房数据
					loupanItemcount.addCol(_cols[1]);
					loupanItemcount.addCol(_cols[1]+"NUM", _cols[2]);
					loupanItemcount.breakRow();
					allB++;
					allitems=allitems+Integer.parseInt(_cols[2]);
				}
				
			 else if (LOUPANITEMCOUNTONLINE.equals(_cols[0])) {// 新房数据
				 loupanItemcountonline.addCol(_cols[1]);
				 loupanItemcountonline.addCol(_cols[1]+"NUM", _cols[2]);
				 loupanItemcountonline.breakRow();
				 allBonline++;
				allitemsonline=allitemsonline+Integer.parseInt(_cols[2]);
			
			
		} else if (LOUPANITEMCOUNTOFFLINE.equals(_cols[0])) {// 新房数据
			loupanItemcountoffline.addCol(_cols[1]);
			loupanItemcountoffline.addCol(_cols[1]+"NUM", _cols[2]);
			loupanItemcountoffline.breakRow();
			allBoffline++;
			allitemsoffline=allitemsoffline+Integer.parseInt(_cols[2]);
		}
		 else if (LOUPANCITY.equals(_cols[0])) {// 新房数据
			 loupanCity.addCol(_cols[1]);
			 loupanCity.addCol(_cols[1]+"NUM", _cols[2]);
			 loupanCity.breakRow();
			}
				
				
			}//for
			loupanItemcount.addCol("楼盘数量NUM","楼盘数量："+String.valueOf(allB));
			loupanItemcount.addCol("商品数量GMV","挂靠商品数量："+String.valueOf(allitems));
			loupanItemcount.breakRow();
			
			loupanItemcountonline.addCol("楼盘数量NUM","楼盘数量："+String.valueOf(allBonline));
			loupanItemcountonline.addCol("商品数量GMV","挂靠在线商品数量："+String.valueOf(allitemsonline));
			loupanItemcountonline.breakRow();
			
			
			loupanItemcountoffline.addCol("楼盘数量NUM","楼盘数量："+String.valueOf(allBoffline));
			loupanItemcountoffline.addCol("商品数量GMV","挂靠下架商品数量："+String.valueOf(allitemsoffline));
			loupanItemcountoffline.breakRow();
			
		}//if
		XmlReportFactory.dump(report, new FileOutputStream(input + ".xml"));


	}

}
