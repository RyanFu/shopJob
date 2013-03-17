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
	public static final String LIST= "list"; // 头数据
	public static final String BUILDING_DETAIL = "buildingDetail"; // 头数据
	public static final String ITEM_DETAIL = "itemDetail"; // 头数据
	public static final String LIST_IN = "listIn"; // 头数据
	public static final String BUILDING_DETAIL_IN = "buildingDetailIn"; // 头数据
	public static final String ITEM_DETAIL_IN = "itemDetailIn"; // 头数据
	
	
	public static final String LIST_PAGE = "http://house.taobao.com/building/list.htm"; // 用于排除本页
	public static final String BUILDING_DETAIL_PAGE = "http://house.taobao.com/building/detail.htm"; // 用于排除本页
	public static final String ITEM_DETAIL_PAGE = "http://house.taobao.com/item/item.htm"; // 用于排除本页
	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
//		String input = "D:\\20120705"; // 文件路径
		String mainName = "淘宝房产"; // 报表名称
	 	String input = args[0]; // 文件路径
		if (!new File(input).exists()) {
			System.out.println("File Not Exist ! => " + input);
			return;
		}

		Report report = Report.newReport(mainName + "垂直项目页面访问数据统计");
		
		Table pv_uv = report.newGroupTable("Pv_Uv",
				"页面浏览统计", "纬度", "数量");
		
		Table listVisit = report.newViewTable("listVisit", mainName
				+ "楼盘list UV来源");
		listVisit.addCol("维度").addCol("数量").addCol(Report.BREAK_VALUE);
		
		Table listDetailVisit = report.newViewTable("listDetailVisit", mainName
				+ "楼盘detail UV来源");
		listDetailVisit.addCol("维度").addCol("数量").addCol(Report.BREAK_VALUE);
		
		Table itemDetailVisit = report.newViewTable("itemDetailVisit", mainName
				+ "宝贝detail UV来源");
		itemDetailVisit.addCol("维度").addCol("数量").addCol(Report.BREAK_VALUE);
		
		List<String> lines = Utils.readWithCharset(input, "utf-8");
		if (lines != null && lines.size() > 0) {
			for (String line : lines) {
				String[] _cols = StringUtils.splitPreserveAllTokens(line,
						CTRL_A);
				if (LIST.equals(_cols[0])) {// 新房数据					
					pv_uv.addCol(Report.newValue("list PV", _cols[1]));
					pv_uv.addCol(Report.newValue("list UV", _cols[2]));
					
				} else if (BUILDING_DETAIL.equals(_cols[0])) {// 新房数据
					pv_uv.addCol(Report.newValue("楼盘detail PV", _cols[1]));
					pv_uv.addCol(Report.newValue("楼盘detail UV", _cols[2]));
					
				} else if (ITEM_DETAIL.equals(_cols[0])) {// 新房数据
					pv_uv.addCol(Report.newValue("宝贝detail PV", _cols[1]));
					pv_uv.addCol(Report.newValue("宝贝detail UV", _cols[2]));
				}else if(LIST_IN.equals(_cols[0])){
					if(LIST_PAGE.equals(_cols[1])){
						listVisit.addCol("本页点击：");
						listVisit.addCol(_cols[2]);
						listVisit.breakRow();
					}else if(_cols[1].isEmpty()){
						listVisit.addCol("未知来源：");
						listVisit.addCol(_cols[2]);
						listVisit.breakRow();
					}else{
						listVisit.addCol(_cols[1]);
						listVisit.addCol(_cols[2]);
						listVisit.breakRow();
					}
				}else if(BUILDING_DETAIL_IN.equals(_cols[0])){
					if(BUILDING_DETAIL_PAGE.equals(_cols[1])){
						listDetailVisit.addCol("本页点击：");
						listDetailVisit.addCol(_cols[2]);
						listDetailVisit.breakRow();
					}else if(_cols[1].isEmpty()){
						listDetailVisit.addCol("未知来源：");
						listDetailVisit.addCol(_cols[2]);
						listDetailVisit.breakRow();
					}else{
						listDetailVisit.addCol(_cols[1]);
						listDetailVisit.addCol(_cols[2]);
						listDetailVisit.breakRow();
					}
					
				}else if(ITEM_DETAIL_IN.equals(_cols[0])){
					if(ITEM_DETAIL_PAGE.equals(_cols[1])){
						itemDetailVisit.addCol("本页点击：");
						itemDetailVisit.addCol(_cols[2]);
						itemDetailVisit.breakRow();
					}else if(_cols[1].isEmpty()){
						itemDetailVisit.addCol("未知来源：");
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
