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
public class FangAACount {
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

		String AUCTIONSTATUS = "auction_status"; // 头数据
		Report report = Report.newReport(mainName + "商品报表");
		Table allAuctionByStatus = report.newGroupTable("auction_status",
				"淘宝房产商品数量统计", "纬度", "数量");
		// 按类目商品数目
		String LEIMU = "leimu"; // 头数据
		Table allAuctionByLeimu = report.newGroupTable("leimu", "按类目统计商品数量",
				"纬度", "数量");
		// 按类目统计shang架商品数目
				String MEILUONLINE = "meiluonline"; // 头数据
				Table allAuctionByLeimuOnline = report.newGroupTable("meiluonline",
						"按类目统计上架商品数量", "纬度", "数量");
		// 按类目统计下架商品数目
		String MEILUDOWN = "meiludown"; // 头数据
		Table allAuctionByLeimuDown = report.newGroupTable("meiludown",
				"按类目统计下架商品数量", "纬度", "数量");
		
		// 按城市统计
		String CITY = "city"; // 头数据
		Table allAuctionByCity = report.newGroupTable("city", "按城市统计总的商品数量",
				"纬度", "数量");
		// 按城市统计在线商品数目
		String CITYONLINE = "cityonline"; // 头数据
		Table allAuctionByCityOnline = report.newGroupTable("cityonline",
				"按城市统计在线商品数量", "纬度", "数量");

		// 按城市统计下架商品数目
		String CITYDOWNLINE = "citydownline"; // 头数据
		Table allAuctionByCityDownline = report.newGroupTable("citydownline",
				"按城市统计下架商品数量", "纬度", "数量");
		// 按卖家统计商品数目
		String SELLER = "seller"; // 头数据
		Table allAuctionBySeller = report.newGroupTable("seller", "按卖家统计商品数量",
				"纬度", "数量");

		// 按卖家统计商品数目
		String SELLERONLINE = "selleronline"; // 头数据
		Table allAuctionBySellerOnline = report.newGroupTable("selleronline",
				"按卖家统计在线商品数量", "纬度", "数量");

		List<String> lines = Utils.readWithCharset(input, "utf-8");
		// 做循环！
		if (lines != null && lines.size() > 0) {
			long allOnlineAuctions = 0;// 所有在线商品
			long allNotOnLineAuctions = 0;// 所有下架的商品
			long ccAuctions = 0;
			
			for (String line : lines) {
				String[] _cols = StringUtils.splitPreserveAllTokens(line,
						CTRL_A);
				if (AUCTIONSTATUS.equals(_cols[0])) {// 说明是商品总数 根据状态维度的
					if (_cols[1].equals("0") || _cols[1].equals("1")) {
						allOnlineAuctions = allOnlineAuctions
								+ Long.parseLong(_cols[2]);
					} else if (_cols[1].equals("-2") || _cols[1].equals("-3")) {
						allNotOnLineAuctions = allNotOnLineAuctions
								+ Long.parseLong(_cols[2]);
					} else if (_cols[1].equals("-9")) {
						ccAuctions = ccAuctions + Long.parseLong(_cols[2]);
					}

				}// 根据状态维度完成！
					// -------------------------根据城市维度
				else if (CITY.equals(_cols[0])) {//
					allAuctionByCity
							.addCol(Report.newValue(_cols[1], _cols[2]));

				}//
					// -------------------------根据城市维度同时在线商品数目
				else if (CITYDOWNLINE.equals(_cols[0])) {//
					allAuctionByCityDownline.addCol(Report.newValue(_cols[1],
							_cols[2]));

				}//
					// -------------------------根据城市维度同时下架商品数目
				else if (CITYONLINE.equals(_cols[0])) {//
					allAuctionByCityOnline.addCol(Report.newValue(_cols[1],
							_cols[2]));

				}//
					// -------------------------根据类目维度统计数据
				else if (LEIMU.equals(_cols[0])) {//
					allAuctionByLeimu.addCol(Report.newValue(
							getLeiMuNale(_cols[1]), _cols[2]));

				}//

				// -------------------------根据类目下架维度统计数据
				else if (MEILUDOWN.equals(_cols[0])) {//
					allAuctionByLeimuDown.addCol(Report.newValue(
							getLeiMuNale(_cols[1]), _cols[2]));

				}//
					// -------------------------根据类目上架维度统计数据
				else if (MEILUONLINE.equals(_cols[0])) {//
					allAuctionByLeimuOnline.addCol(Report.newValue(
							getLeiMuNale(_cols[1]), _cols[2]));

				}//

				// -------------------------根据用户每个用户的数目
				else if (SELLER.equals(_cols[0])) {//
					allAuctionBySeller.addCol(Report.newValue(_cols[1],
							_cols[2]));

				}//

				// -------------------------根据用户上线的数目
				else if (SELLERONLINE.equals(_cols[0])) {//
					allAuctionBySellerOnline.addCol(Report.newValue(_cols[1],
							_cols[2]));

				}//

			}// for

			allAuctionByStatus.addCol(Report.newValue(
					"总数",
					String.valueOf(allOnlineAuctions + allNotOnLineAuctions
							+ ccAuctions)));
			allAuctionByStatus.addCol(Report.newValue("在线商品数量",
					String.valueOf(allOnlineAuctions)));
			allAuctionByStatus.addCol(Report.newValue("下架商品数量",
					String.valueOf(allNotOnLineAuctions)));
			allAuctionByStatus.addCol(Report.newValue("CC数量",
					String.valueOf(ccAuctions)));
		}// if
		XmlReportFactory.dump(report, new FileOutputStream(input + ".xml"));

	}

	public static String getLeiMuNale(String string) {
		
		if("50023902".equals(string.trim())){
			return "其他房产服务";
		}
		else if("50023579".equals(string.trim())){
			return "委托服务/中介服务";
		}
		else if("50023597".equals(string.trim())){
			return "租房/出租/租赁/日租短租 ";
		}
		else if("50023598".equals(string.trim())){
			return "二手房/出售/卖房";
		}
		else if("50023944".equals(string.trim())){
			return "新房/新楼盘/一手房";
		}
		else if("50026396".equals(string.trim())){
			return "垂直市场类目";
		}
		return "类目未知";
		
		 
		
		
		
		
		
	}
}
