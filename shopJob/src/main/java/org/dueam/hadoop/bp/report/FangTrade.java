/**   
 *********************
 *                                  * 
 *   淘宝房产，版权所有！  *
 *                                  * 
 *     fang.taobao.com       *
 *********************
 * @Title: FangCount.java 
 * @Package org.dueam.hadoop.bp.report 
 * @Description: TODO(用一句话描述该文件做什么) 
 * @author tiance
 * @date 2011-11-16 上午11:02:18 
 * @version V1.0   
 */
package org.dueam.hadoop.bp.report;

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
 * @ClassName: FangCount
 * @Description: TODO(这里用一句话描述这个类的作用)
 * @author tiance
 * @date 2011-11-16 上午11:02:18
 * @version 1.0
 */
public class FangTrade {

	private static char CTRL_A = (char) 0x01;

	/**
	 * 
	 * @Title: main
	 * @Description: TODO(这里用一句话描述这个方法的作用)
	 * @param @param args 设定文件
	 * @return void 返回类型
	 * @throws
	 */
	public static void main(String[] args) throws IOException {
		//String input = "C:\\Users\\longjia.zt\\Documents\\20120715"; // 文件路径
		String mainName = "淘宝房产"; // 报表名称
		String input = args[0]; // 文件路径
		// String mainName = args[1]; //报表名称
		Report report = Report.newReport(mainName + "预约、交易数据");

		String smallOrder2011 = "2011smallYear";//
		String smallOrder2011_today = "2012smallYear";//

		String bigOrder2011 = "2011bigyear";//
		String bigOrder2011_today = "2012bigyear";//

		String orderCount = "ordercount";// 小定订单订单数 排行(卖家名、成交笔数、成交金额)
		String appointmentCount = "appointmentcount";// 预约数排行（卖家名、预约数）
		if (!new File(input).exists()) {
			System.out.println("File Not Exist ! => " + input);
			return;
		}
		List<String> lines = Utils.readWithCharset(input, "utf-8");
		
		Table smallNewPosTable = report.newViewTable("smallNewPosTable", mainName
				+ "新小定成交排行榜");
		smallNewPosTable.addCol("卖家用户名").addCol("商品ID").addCol("成交笔数").addCol("支付宝成交金额")
				.addCol(Report.BREAK_VALUE);


		Table bigNewPosTable = report.newViewTable("bigNewPosTable", mainName
				+ "新大定成交排行榜");
		bigNewPosTable.addCol("卖家用户名").addCol("商品ID").addCol("成交笔数").addCol("支付宝成交金额")
				.addCol(Report.BREAK_VALUE);
		
		Table orderCountTable = report.newViewTable(orderCount, mainName
				+ "所有订单数排行榜");
		orderCountTable.addCol("卖家用户名").addCol("成交笔数").addCol("GMV金额")
				.addCol(Report.BREAK_VALUE);

		Table appointmentCountTable = report.newViewTable("appointmentCount",
				"预约数排行");
		appointmentCountTable.addCol("卖家昵称").addCol("预约条数").addCol("所属类目")
				.addCol(Report.BREAK_VALUE);
		Table smallOrderTable2011 = report.newViewTable(smallOrder2011, mainName
				+ "2011年小定成交排行榜");
		smallOrderTable2011.addCol("卖家用户名").addCol("成交笔数").addCol("支付宝成交金额")
				.addCol(Report.BREAK_VALUE);
		
		Table smallOrderTable2012_today = report.newViewTable(smallOrder2011_today,
				mainName + "2012至今年通联POS小定成交排行榜");
		smallOrderTable2012_today.addCol("卖家用户名").addCol("成交笔数")
				.addCol("支付宝成交金额").addCol(Report.BREAK_VALUE);

		Table bigOrderTable2011 = report.newViewTable(bigOrder2011, mainName
				+ "2011年大定成交排行榜");
		bigOrderTable2011.addCol("卖家用户名").addCol("成交笔数").addCol("支付宝成交金额")
				.addCol(Report.BREAK_VALUE);
		
		Table bigOrderTable2012_today = report.newViewTable(bigOrder2011_today,
				mainName + "2012至今年通联POS大定成交排行榜");
		bigOrderTable2012_today.addCol("卖家用户名").addCol("成交笔数")
				.addCol("支付宝成交金额").addCol(Report.BREAK_VALUE);
		
		
		
		Table a2012smallNewPos = report.newViewTable("2012smallNewPos",
				mainName + "2012至今年新POS小定成交排行榜");
		a2012smallNewPos.addCol("卖家用户名").addCol("成交笔数")
				.addCol("支付宝成交金额").addCol(Report.BREAK_VALUE);
		Table a2012bigNewPos = report.newViewTable("a2012bigNewPos",
				mainName + "2012至今年新POS大定成交排行榜");
		a2012bigNewPos.addCol("卖家用户名").addCol("成交笔数")
				.addCol("支付宝成交金额").addCol(Report.BREAK_VALUE);
		
		if (lines != null && lines.size() > 0) {
			Float smallOrderCountFee = 0.00f;
			int smallOrderCount = 0;
			int appointCount = 0;
			Float smallOrder2011Fee = 0.00f;
			int smallOrder2011Count = 0;
			Float bigOrder2011Fee = 0.00f;
			int bigOrder2011Count = 0;

			
			//新pos小定统计参数
			String preUserNameSmall1="";
			int oneUsersmallNUM1=0;
			Float oneUsersmallGMV1=0.0f;
			int smallDoneCount1 = 0;
			Float smallOrderFee1 = 0.00f;
	
			
			//新pos统计大定参数
			String preUserNameBig1="";
			int oneUserbigNUM1=0;
			Float oneUserbigGMV1=0.0f;
			int bigDoneCount1 = 0;
			Float bigOrderFee1 = 0.00f;
			
			//通联pos2012 to-today 通联pos 小定
			Float smallOrder2012_todayFee = 0.00f;
			int smallOrder2012_todayCount = 0;
			
			//新POS2012 to-today 通联pos 小定
			Float smallOrder2012_todayFee1 = 0.00f;
			int smallOrder2012_todayCount1 = 0;
			
			//通联pos2012 to-today 新pos大定
			Float bigOrder2012_todayFee = 0.00f;
			int bigOrder2012_todayCount = 0;
			
			//新POS20122012 to-today 新pos大定
			Float bigOrder2012_todayFee1 = 0.00f;
			int bigOrder2012_todayCount1 = 0;
			
			for (String line : lines) {
				String[] _cols = StringUtils.splitPreserveAllTokens(line,
						CTRL_A);
				if((_cols!=null&&_cols.length>2)&&(_cols[1].contains("测试") ||_cols[1].contains("龙甲"))){
					continue;
				}
				
			 if (line.startsWith("smallNewPos")) {// 小定成交排行榜
					
					if (_cols.length > 0) {
						
						if(StringUtils.isEmpty(preUserNameSmall1)||StringUtils.equals(preUserNameSmall1,_cols[1])){
							oneUsersmallNUM1=oneUsersmallNUM1+ Integer.valueOf(_cols[3]);
							oneUsersmallGMV1=oneUsersmallGMV1+Float.parseFloat(_cols[4]);
						}else{
							
							smallNewPosTable.addCol(preUserNameSmall1+"小合计");
							smallNewPosTable.addCol(preUserNameSmall1 + "合计ITEMID", "");
							smallNewPosTable.addCol(preUserNameSmall1+ "合计数量", oneUsersmallNUM1+"");
							smallNewPosTable.addCol(preUserNameSmall1 + "合计GMV", oneUsersmallGMV1+"");
							smallNewPosTable.breakRow();	
							oneUsersmallNUM1=0;
							oneUsersmallGMV1=0.0f;
							oneUsersmallNUM1=oneUsersmallNUM1+ Integer.valueOf(_cols[3]);
							oneUsersmallGMV1=oneUsersmallGMV1+Float.parseFloat(_cols[4]);
						}
										
						smallNewPosTable.addCol(_cols[1]);
						smallNewPosTable.addCol(_cols[1] + "ITEMID", _cols[2]);
						smallNewPosTable.addCol(_cols[1] + "NUM", _cols[3]);
						smallNewPosTable.addCol(_cols[1] + "GMV", _cols[4]);
						smallNewPosTable.breakRow();
						
						preUserNameSmall1=_cols[1];
						
						smallDoneCount1 = smallDoneCount1
								+ Integer.valueOf(_cols[3]);
						smallOrderFee1 = smallOrderFee1
								+ Float.parseFloat(_cols[4]);
					}
				} 
				else if (line.startsWith("bigNewPos")) {

					if (_cols.length > 0) {
						
						if(StringUtils.isEmpty(preUserNameBig1)||StringUtils.equals(preUserNameBig1,_cols[1])){
							oneUserbigNUM1=oneUserbigNUM1+ Integer.valueOf(_cols[3]);
							oneUserbigGMV1=oneUserbigGMV1+Float.parseFloat(_cols[4]);
						}else{
							
							bigNewPosTable.addCol(preUserNameBig1+"小合计");
							bigNewPosTable.addCol(preUserNameBig1 + "合计ITEMID", "");
							bigNewPosTable.addCol(preUserNameBig1+ "合计数量", oneUserbigNUM1+"");
							bigNewPosTable.addCol(preUserNameBig1 + "合计GMV", oneUserbigGMV1+"");
							bigNewPosTable.breakRow();	
							oneUserbigNUM1=0;
							oneUserbigGMV1=0.0f;
							oneUserbigNUM1=oneUserbigNUM1+ Integer.valueOf(_cols[3]);
							oneUserbigGMV1=oneUserbigGMV1+Float.parseFloat(_cols[4]);
						}						
						
						bigNewPosTable.addCol(_cols[1]);
						bigNewPosTable.addCol(_cols[1] + "ITEMID", _cols[2]);
						bigNewPosTable.addCol(_cols[1] + "NUM", _cols[3]);
						bigNewPosTable.addCol(_cols[1] + "GMV", _cols[4]);
						bigNewPosTable.breakRow();
						
						preUserNameBig1=_cols[1];
						
						bigDoneCount1 = bigDoneCount1 + Integer.valueOf(_cols[3]);
						bigOrderFee1 = bigOrderFee1 + Float.parseFloat(_cols[4]);
					}
				}
				
				else if (line.startsWith(orderCount)) {

					if (_cols.length > 0) {
						orderCountTable.addCol(_cols[1]);
						orderCountTable.addCol(_cols[1] + "NUM", _cols[2]);
						orderCountTable.addCol(_cols[1] + "GMV", _cols[3]);
						orderCountTable.breakRow();
						smallOrderCount = smallOrderCount
								+ Integer.valueOf(_cols[2]);
						smallOrderCountFee = smallOrderCountFee
								+ Float.parseFloat(_cols[3]);
					}
				} else if (line.startsWith(appointmentCount)) {

					if (_cols.length > 0) {
						appointmentCountTable.addCol(_cols[1]);
						appointmentCountTable.addCol(_cols[1] + "COUNT",
								_cols[2]);
						appointmentCountTable.addCol(_cols[1] + "LEIMU",
								FangAACount.getLeiMuNale(_cols[3]));
						appointmentCountTable.breakRow();
						appointCount = appointCount + Integer.valueOf(_cols[2]);
					}
				} else if (line.startsWith(smallOrder2011)) {// 2011小定

					smallOrderTable2011.addCol(_cols[1]);
					smallOrderTable2011.addCol(_cols[1] + "NUM", _cols[2]);
					smallOrderTable2011.addCol(_cols[1] + "GMV", _cols[3]);
					smallOrderTable2011.breakRow();
					smallOrder2011Count = smallOrder2011Count + Integer.valueOf(_cols[2]);
					smallOrder2011Fee = smallOrder2011Fee + Float.parseFloat(_cols[3]);
					
					
				} else if (line.startsWith(smallOrder2011_today)) {// 2011小定
		
					smallOrderTable2012_today.addCol(_cols[1]);
					smallOrderTable2012_today.addCol(_cols[1] + "NUM", _cols[2]);
					smallOrderTable2012_today.addCol(_cols[1] + "GMV", _cols[3]);
					smallOrderTable2012_today.breakRow();
					smallOrder2012_todayCount = smallOrder2012_todayCount + Integer.valueOf(_cols[2]);
					smallOrder2012_todayFee = smallOrder2012_todayFee + Float.parseFloat(_cols[3]);
				}
				
				else if (line.startsWith("2012smallNewPos")) {// 2011小定
					
					a2012smallNewPos.addCol(_cols[1]);
					a2012smallNewPos.addCol(_cols[1] + "NUM", _cols[2]);
					a2012smallNewPos.addCol(_cols[1] + "GMV", _cols[3]);
					a2012smallNewPos.breakRow();
					smallOrder2012_todayCount1 = smallOrder2012_todayCount1 + Integer.valueOf(_cols[2]);
					smallOrder2012_todayFee1 = smallOrder2012_todayFee1 + Float.parseFloat(_cols[3]);
				}
				
				else if (line.startsWith(bigOrder2011)) {// 2011大定
				
					bigOrderTable2011.addCol(_cols[1]);
					bigOrderTable2011.addCol(_cols[1] + "NUM", _cols[2]);
					bigOrderTable2011.addCol(_cols[1] + "GMV", _cols[3]);
					bigOrderTable2011.breakRow();
					bigOrder2011Count = bigOrder2011Count + Integer.valueOf(_cols[2]);
					bigOrder2011Fee = bigOrder2011Fee + Float.parseFloat(_cols[3]);
				} 
				else if (line.startsWith(bigOrder2011_today)) {// 2011大定
		
					bigOrderTable2012_today.addCol(_cols[1]);
					bigOrderTable2012_today.addCol(_cols[1] + "NUM", _cols[2]);
					bigOrderTable2012_today.addCol(_cols[1] + "GMV", _cols[3]);
					bigOrderTable2012_today.breakRow();
					bigOrder2012_todayCount = bigOrder2012_todayCount + Integer.valueOf(_cols[2]);
					bigOrder2012_todayFee = bigOrder2012_todayFee + Float.parseFloat(_cols[3]);
				}
				else if (line.startsWith("2012bigNewPos")) {// 2011大定
					
					a2012bigNewPos.addCol(_cols[1]);
					a2012bigNewPos.addCol(_cols[1] + "NUM", _cols[2]);
					a2012bigNewPos.addCol(_cols[1] + "GMV", _cols[3]);
					a2012bigNewPos.breakRow();
					bigOrder2012_todayCount1 = bigOrder2012_todayCount1 + Integer.valueOf(_cols[2]);
					bigOrder2012_todayFee1 = bigOrder2012_todayFee1 + Float.parseFloat(_cols[3]);
				}
				//TODO
				
			}
			
		 
			
			smallNewPosTable.addCol(preUserNameSmall1+"小合计");
			smallNewPosTable.addCol(preUserNameSmall1 + "合计ITEMID", "");
			smallNewPosTable.addCol(preUserNameSmall1+ "合计数量", oneUsersmallNUM1+"");
			smallNewPosTable.addCol(preUserNameSmall1 + "合计GMV", oneUsersmallGMV1+"");
			smallNewPosTable.breakRow();	
			
			
			smallNewPosTable.addCol("小定成交合计");
			smallNewPosTable.addCol("");
			smallNewPosTable.addCol("小定成交合计NUM", String.valueOf(smallDoneCount1));
			smallNewPosTable.addCol("小定成交合计GMV", String.valueOf(smallOrderFee1));
			smallNewPosTable.breakRow();
			
	
			bigNewPosTable.addCol(preUserNameBig1+"小合计");
			bigNewPosTable.addCol(preUserNameBig1 + "合计ITEMID", "");
			bigNewPosTable.addCol(preUserNameBig1+ "合计数量", oneUserbigNUM1+"");
			bigNewPosTable.addCol(preUserNameBig1 + "合计GMV", oneUserbigGMV1+"");
			bigNewPosTable.breakRow();	
			
			bigNewPosTable.addCol("大定成交合计");
			bigNewPosTable.addCol(" ");
			bigNewPosTable.addCol("大定成交合计NUM", String.valueOf(bigDoneCount1));
			bigNewPosTable.addCol("大定成交合计GMV", String.valueOf(bigOrderFee1));
			bigNewPosTable.breakRow();
			
			orderCountTable.addCol("小定订单数");
			orderCountTable.addCol("小定订单数NUM", String.valueOf(smallOrderCount));
			orderCountTable.addCol("小定订单数GMV",
					String.valueOf(smallOrderCountFee));
			orderCountTable.breakRow();

			appointmentCountTable.addCol("预约合计：");
			appointmentCountTable.addCol(String.valueOf(appointCount));
			orderCountTable.breakRow();

			
			smallOrderTable2011.addCol("2011小定成交合计");
			smallOrderTable2011.addCol("2011小定成交合计NUM", String.valueOf(smallOrder2011Count));
			smallOrderTable2011.addCol("2011小定成交合计GMV", String.valueOf(smallOrder2011Fee.longValue()));
			smallOrderTable2011.breakRow();
			
			smallOrderTable2012_today.addCol("2012至今小定成交合计");
			smallOrderTable2012_today.addCol("2012至今小定成交合计NUM", String.valueOf(smallOrder2012_todayCount));
			smallOrderTable2012_today.addCol("2012至今小定成交合计GMV", String.valueOf(smallOrder2012_todayFee.longValue()));
			smallOrderTable2012_today.breakRow();
			
			bigOrderTable2011.addCol("2011通联POS大定成交合计");
			bigOrderTable2011.addCol("2011通联POS大定成交合计NUM", String.valueOf(bigOrder2011Count));
			bigOrderTable2011.addCol("2011通联POS大定成交合计GMV", String.valueOf(bigOrder2011Fee.longValue()));
			bigOrderTable2011.breakRow();
			 
			bigOrderTable2012_today.addCol("2012通联POS至今大定成交合计");
			bigOrderTable2012_today.addCol("2012通联POS至今大定成交合计NUM", String.valueOf(bigOrder2012_todayCount));
			bigOrderTable2012_today.addCol("2012通联POS至今大定成交合计GMV", String.valueOf(bigOrder2012_todayFee.longValue()));
			bigOrderTable2012_today.breakRow();
			
			a2012smallNewPos.addCol("2012新pos至今小定成交合计");
			a2012smallNewPos.addCol("2012新pos至今小定成交合计NUM", String.valueOf(smallOrder2012_todayCount1));
			a2012smallNewPos.addCol("2012新pos至今小定成交合计GMV", String.valueOf(smallOrder2012_todayFee1.longValue()));
			a2012smallNewPos.breakRow();
			
			a2012bigNewPos.addCol("2012新pos至今大定成交合计");
			a2012bigNewPos.addCol("2012新pos至今大定成交合计NUM", String.valueOf(bigOrder2012_todayCount1));
			a2012bigNewPos.addCol("2012新pos至今大定成交合计GMV", String.valueOf(bigOrder2012_todayFee1.longValue()));
			a2012bigNewPos.breakRow();
		}
		XmlReportFactory.dump(report, new FileOutputStream(input + ".xml"));
	}
}
