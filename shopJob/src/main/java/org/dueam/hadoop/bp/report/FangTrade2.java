/**
 * 
 */
package org.dueam.hadoop.bp.report;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.dueam.hadoop.common.util.MapUtils;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;
/**
 * @author longjia.zt
 *
 */
public class FangTrade2 {
	private static char CTRL_A = (char) 0x01;
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		    //String input = "C:\\Users\\longjia.zt\\Documents\\20120222"; // 文件路径
				String mainName = "淘宝房产"; // 报表名称
			 	String input = args[0]; // 文件路径
				// String mainName = args[1]; //报表名称

				if (!new File(input).exists()) {
					System.out.println("File Not Exist ! => " + input);
					return;
				}
				Report report = Report.newReport(mainName + "交易报表");
				//交易总的
				String ALLTRADES = "alltrades";//所有交易笔数
				String ALLTRADESGMV = "alltradesGMV";//所有交易GMV
				String TRADEUSER = "tradeUser";//所有的交易人  去重！
				String ALLSUCCESSTRADE = "allsuccesstrade";//交易成功笔数
				String ALLSUCCESSM = "allsuccessM";//交易成功金额
				Table totalGMV = report.newTable("totalGMV",
						"淘宝房产交易总计", "纬度", "数量");
		
				String LEIMUM="leimuM";
				Table leimuSuccessM = report.newGroupTable("leimuSuccessM",
						"二级类目已成功交易统计", "纬度", "金额");
				
				String LEIMUNUM="leimuNum";
				Table leimuSuccessMUM = report.newGroupTable("leimuSuccessMUM",
						"二级类目已成功交易笔数统计", "纬度", "数量");
				
				
				String LEIMUBUYNUM="leimubuyNum";
				Table leimuSuccessBuyUser = report.newGroupTable("leimuSuccessBuyUser",
						"二级类目已成功交易人数统计", "纬度", "数量");

				String LEIMUGMV="leimuGMV";
				Table leimuGMV = report.newGroupTable("leimuGMV",
						"二级类目GMV交易统计", "纬度", "金额");
				
				List<String> lines = Utils.readWithCharset(input, "utf-8");
				// 做循环！
				if (lines != null && lines.size() > 0) {
					long tradeUsers = 0; 
					//统计每个类目下面的人数
					int others=0;//50023902
					int zhongjie=0;//50023579
					int chuzhu=0;//50023597
					int xinfang=0;//50023944
					int ershoufang=0;//50023598
					
					for (String line : lines) {
						String[] _cols = StringUtils.splitPreserveAllTokens(line,
								CTRL_A);
						if (ALLTRADES.equals(_cols[0])) {
							totalGMV.addCol(Report.newValue("GMV交易笔数统计",_cols[1]));

						}// 
						
						else if (ALLTRADESGMV.equals(_cols[0])) { 
							totalGMV.addCol(Report.newValue("GMV交易金额统计",_cols[1]));

						}// 
						else if (TRADEUSER.equals(_cols[0])) { 
							tradeUsers=tradeUsers+1;

						}
						else if (ALLSUCCESSTRADE.equals(_cols[0])) {// 说明是商品总数 根据状态维度的
							totalGMV.addCol(Report.newValue("已成功交易笔数统计",_cols[1]));

						}//
						
						else if (ALLSUCCESSM.equals(_cols[0])) {// 说明是商品总数 根据状态维度的
							totalGMV.addCol(Report.newValue("已成功交易金额统计",_cols[1]));

						}//
						
						else if (LEIMUM.equals(_cols[0])) {// 说明是商品总数 根据状态维度的
							leimuSuccessM.addCol(Report.newValue(FangAACount.getLeiMuNale(_cols[1]),_cols[2]));
						}//
						
						else if (LEIMUNUM.equals(_cols[0])) {// 说明是商品总数 根据状态维度的
							leimuSuccessMUM.addCol(Report.newValue(FangAACount.getLeiMuNale(_cols[1]),_cols[2]));
						}//
						
						else if (LEIMUBUYNUM.equals(_cols[0])) {// 说明是商品总数 根据状态维度的
							
							if("50023902".equals(_cols[1])){
								others=others+1;
							}
							else if("50023579".equals(_cols[1])){
								zhongjie=zhongjie+1;
							}
							else if("50023597".equals(_cols[1])){
								chuzhu=chuzhu+1;
							}
							else if("50023944".equals(_cols[1])){
								xinfang=xinfang+1;
							}
							else if("50023598".equals(_cols[1])){
								ershoufang=ershoufang+1;
							}
							
							
							
						}//
						
						else if (LEIMUGMV.equals(_cols[0])) {// 说明是商品总数 根据状态维度的
							leimuGMV.addCol(Report.newValue(FangAACount.getLeiMuNale(_cols[1]),_cols[2]));
						}//
						
					}// for

				totalGMV.addCol(Report.newValue("共参与交易人数统计",tradeUsers+""));
				leimuSuccessBuyUser.addCol(Report.newValue("租房/出租/租赁/日租短租",chuzhu+""));
				leimuSuccessBuyUser.addCol(Report.newValue("二手房/出售/卖房",ershoufang+""));
				leimuSuccessBuyUser.addCol(Report.newValue("新房/新楼盘/一手房",xinfang+""));
				leimuSuccessBuyUser.addCol(Report.newValue("委托服务/中介服务",zhongjie+""));
				leimuSuccessBuyUser.addCol(Report.newValue("其他房产服务",others+""));
				}// if
				XmlReportFactory.dump(report, new FileOutputStream(input + ".xml"));

			}


}
