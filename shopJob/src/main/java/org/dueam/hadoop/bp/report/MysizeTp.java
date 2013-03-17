package org.dueam.hadoop.bp.report;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.dueam.hadoop.common.util.Fmt;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

/**
 * 尺码库相关数据统计
 */
public class MysizeTp {
	private static char TAB = (char) 0x09;
	// 第一个故意为空的
	static String[] partName = new String[] { "", "身高", "体重", "肩宽", "胸围", "腰围", "臀围", "脚长", "脚宽", "下胸围", "胸围差" };
	private static char CTRL_A = (char) 0x01;

	/**
	 * @param args
	 * @throws java.io.IOException
	 */
	public static void main(String[] args) throws IOException {
		String input = args[0];
		if (!new File(input).exists()) {
			System.out.println("File Not Exist ! => " + input);
			return;
		}

		Report report = Report.newReport("尺码模板相关数据统计");

		Table tpGroup = report.newGroupTable("tpGroup", "被启用的服装模板的各种组合统计(只展示大于50的)");

		Table countSizeSKU = report.newGroupTable("countSizeSKU", "被启用的单个服装模板的尺码个数分布统计");

		Table countTp = report.newGroupTable("countTp", "单个卖家启用的模板个数分组");

		Table xiePart = report.newViewTable("xiePart", "鞋子模板的两种模板比例");
		xiePart.addCol("含有脚宽的模板数").addCol("鞋子模板总数").addCol("所占比率").addCol(Report.BREAK_VALUE);


		Table tpCount = report.newGroupTable("tpCount", "所有模板个数");
		Table tpCountByType = report.newGroupTable("tpCountByType", "所有模板个数按类型分");
		Table tpInUseCount = report.newGroupTable("tpInUseCount", "所有被使用的模板个数");
		Table tpInUseCountByType = report.newGroupTable("tpInUseCountByType", "所有被使用模板个数按类型分");
		Table tpAuctionCount = report.newGroupTable("tpAuctionCount", "所有使用模板的[宝贝]个数");
		Table tpAuctionCountByCat = report.newGroupTable("tpAuctionCountByCat", "所有使用模板的[宝贝]个数按类目分");
		Table userCountByCat = report.newGroupTable("userCountByCat", "所有使用模板的[卖家]个数按类目分");




		for (String line : Utils.readWithCharset(input, "utf-8")) {
			if (line == null)
				return;
			String[] _allCols = StringUtils.splitByWholeSeparator(line, "\t");
			String[] _allCols_1 = StringUtils.splitPreserveAllTokens(line, CTRL_A);

			String type = _allCols[0];
			String type_1 = _allCols_1[0];


			if (type_1.equals("tpCount")) {
				tpCount.addCol("所有模板个数", _allCols_1[1]);
			}
			if (type_1.equals("tpCountByType")) {
				tpCountByType.addCol(convertTpType(_allCols_1[1]), _allCols_1[2]);
			}
			if (type_1.equals("tpInUseCount")) {
				tpInUseCount.addCol("所有被使用的模板个数", _allCols_1[1]);
			}
			if (type_1.equals("tpInUseCountByType")) {
				tpInUseCountByType.addCol(convertTpType(_allCols_1[1]), _allCols_1[2]);
			}
			if (type_1.equals("tpAuctionCount")) {
				tpAuctionCount.addCol("所有使用模板的宝贝个数", _allCols_1[1]);
			}
//			if (type_1.equals("tpAuctionCountByCat")) {
//				tpAuctionCountByCat.addCol(convertCatName(_allCols_1[1]), _allCols_1[2]);
//			}
//			if (type_1.equals("userCountByCat")) {
//				userCountByCat.addCol(convertCatName(_allCols_1[1]), _allCols_1[2]);
//			}



			if (type.startsWith("tpGroup-")) {
				if (Integer.valueOf(_allCols[1]) >= 50)// 只展示大于50的
					tpGroup.addCol(convertKey(type), _allCols[1]); // tpGroup
			}
			if (type.startsWith("countSizeSKU-")) {
				countSizeSKU.addCol(type.replace("countSizeSKU-", ""), _allCols[1]); // CountSizeSKU
			}

			if (type.startsWith("countTp-")) {
				countTp.addCol(type.replace("countTp-", ""), _allCols[1]); // countTp
			}

			if (type.startsWith("xiePart-")) {
				xiePart.addCol(type.replace("xiePart-", ""), _allCols[1]); // xiePart
				xiePart.addCol(_allCols[2]).addCol(Fmt.parent2(_allCols[1], _allCols[2])).breakRow();
			}

		}
		//运营要求按他们的顺序排，所以单独处理这块
		String[] catArray={"16","30","50006843","50011740","1625","50008881","50008883","50012029","50013886"};
		
		for(int i =0;i<catArray.length;i++){
			String key=catArray[i];
			
			for (String line : Utils.readWithCharset(input, "utf-8")) {
				if (line == null)
					continue;
				String[] _allCols_1 = StringUtils.splitPreserveAllTokens(line, CTRL_A);

				String type_1 = _allCols_1[0];
				
				if (type_1.equals("tpAuctionCountByCat")&&_allCols_1[1].equals(key)) {
					tpAuctionCountByCat.addCol(convertCatName(_allCols_1[1]), _allCols_1[2]);
				}
				if (type_1.equals("userCountByCat")&&_allCols_1[1].equals(key)) {
					userCountByCat.addCol(convertCatName(_allCols_1[1]), _allCols_1[2]);
				}
				
			}
			
		}
		
		
		
		
		
		countSizeSKU.sort(Table.SORT_VALUE);
		tpGroup.sort(Table.SORT_VALUE);
		countTp.sort(Table.SORT_VALUE);
		XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
	}
	

	/**
	 * 转换key,例如12:身高、体重
	 *
	 * @throws Exception
	 */
	public static String convertKey(String groupKey) {
		String array = groupKey.replace("tpGroup-", "");// 去掉标志符
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < array.getBytes().length; i++) {
			int pointer = Integer.valueOf(array.substring(i, i + 1));
			sb.append(partName[pointer]);
			sb.append("、");
		}

		sb.delete(sb.length() - 1, sb.length());

		return sb.toString();
	}

	/**
	 * 类目名称
	 *
	 * @throws Exception
	 */
	public static String convertCatName(String catId) {
		Map<String, String> map = new HashMap<String, String>();
		map.put("16", "女装/女士精品");
		map.put("30", "男装");
		map.put("50011740", "男鞋");
		map.put("50006843", "女鞋");
		map.put("50012029", "运动鞋new");
		map.put("50013886", "户外/登山/野营/旅行用品");
		map.put("50008881", "文胸");
		map.put("50008883", "文胸套装");
		map.put("1625", "女士内衣/男士内衣/家居服");

		if (map.get(catId) != null) {
			return map.get(catId);
		} else {
			return catId+"类目";
		}
	}


	/**
	 * 模板类型
	 *
	 * @throws Exception
	 */
	public static String convertTpType(String type) {
		Map<String, String> map = new HashMap<String, String>();
		map.put("1", "服装模板");
		map.put("2", "鞋子模板");
		map.put("3", "文胸模板");
		if (map.get(type) != null) {
			return map.get(type);
		} else {
			return "";
		}
	}

}
