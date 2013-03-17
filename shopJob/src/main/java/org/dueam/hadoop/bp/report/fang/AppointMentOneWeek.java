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
	public static final String NEW = "new"; // 头数据
	public static final String ERSHOU = "ershou"; // 头数据
	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		 //String input = "C:\\Users\\longjia.zt\\Documents\\20120517"; // 文件路径
		String mainName = "淘宝房产"; // 报表名称
		String input = args[0]; // 文件路径
		if (!new File(input).exists()) {
			System.out.println("File Not Exist ! => " + input);
			return;
		}
		Report report = Report.newReport(mainName + "周数据【新房 二手房-预约数据】");
		Table newHouseAppointTable = report.newViewTable("newHouseAppoint", mainName
				+ "新房预约周数据");
		newHouseAppointTable.addCol("卖家用户名").addCol("预约笔数")
				.addCol(Report.BREAK_VALUE);
		
		
		Table ershouHouseAppointTable = report.newViewTable("ershouHouseAppoint", mainName
				+ "租房预约数据");
		ershouHouseAppointTable.addCol("卖家用户名").addCol("预约笔数")
				.addCol(Report.BREAK_VALUE);
		
		List<String> lines = Utils.readWithCharset(input, "utf-8");
		if (lines != null && lines.size() > 0) {
			int newHouseAppointCount= 0;
			int ershouHouseAppointCount= 0;
			for (String line : lines) {
				String[] _cols = StringUtils.splitPreserveAllTokens(line,
						CTRL_A);
				if(_cols[1].contains("测试") ||_cols[1].contains("龙甲")){
					continue;
				}
				if (NEW.equals(_cols[0])) {// 新房数据
					newHouseAppointTable.addCol(_cols[1]);
					newHouseAppointTable.addCol(_cols[1]+"NUM", _cols[2]);
					newHouseAppointTable.breakRow();
					newHouseAppointCount = newHouseAppointCount+Integer.valueOf(_cols[2]);				
					
				} else if (ERSHOU.equals(_cols[0])) {// 新房数据
					ershouHouseAppointTable.addCol(_cols[1]);
					ershouHouseAppointTable.addCol(_cols[1]+"NUM", _cols[2]);
					ershouHouseAppointTable.breakRow();
					ershouHouseAppointCount = ershouHouseAppointCount+Integer.valueOf(_cols[2]);				
				
				} 

			}//for
			
			newHouseAppointTable.addCol("新房预约合计");
			newHouseAppointTable.addCol("新房预约合计NUM",String.valueOf(newHouseAppointCount));
			newHouseAppointTable.breakRow();
			
			ershouHouseAppointTable.addCol("租房预约合计");
			ershouHouseAppointTable.addCol("租房预约合计NUM",String.valueOf(ershouHouseAppointCount));
			ershouHouseAppointTable.breakRow();

		}//if
		XmlReportFactory.dump(report, new FileOutputStream(input + ".xml"));


	}

}
