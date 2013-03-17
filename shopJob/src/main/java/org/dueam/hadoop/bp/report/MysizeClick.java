package org.dueam.hadoop.bp.report;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.dueam.hadoop.common.util.Fmt;
import org.dueam.hadoop.common.util.MapUtils;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

/**
 * 尺码库页面点击统计
 */
public class MysizeClick {
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

		Report report = Report.newReport("尺码库页面点击统计及页面PV/UV");

		Table mini = report.newViewTable("detail", "mini页面点击详情");
		mini.addCol("点击位置").addCol("PV").addCol("UV_USER").addCol("PV/UV")
				.breakRow();

		Table profile = report.newViewTable("total", "后台录入页面点击详情");
		profile.addCol("点击位置").addCol("PV").addCol("UV_USER").addCol("PV/UV")
				.breakRow();

		Table tp = report.newViewTable("tp", "尺码模板页面点击详情");
		tp.addCol("点击位置").addCol("PV").addCol("UV_USER").addCol("PV/UV")
				.breakRow();

		Table link = report.newViewTable("link", "mini页面外链接");
		link.addCol("点击位置").addCol("PV").breakRow();


		Table sizeDetail = report.newViewTable("sizeDetail", "尺码评价详情页面PV/UV");
		sizeDetail.addCol("页面").addCol("PV").addCol("UV_USER").addCol("PV/UV")
				.breakRow();

		for (String line : Utils.readWithCharset(input, "utf-8")) {
			if (line == null)
				return;
			String[] _allCols = StringUtils
					.splitPreserveAllTokens(line, CTRL_A);
			String type = _allCols[0];
			// mini
			if (type.startsWith("/tbsizes.1")) {
				mini.addCol(MINI_KEYS.get(type)).addCol(_allCols[1]).addCol(
						_allCols[2]).addCol(Fmt.parent(Long.valueOf(_allCols[1]), Long.valueOf(_allCols[2]))).breakRow();
			}
			// profile
			if (type.startsWith("/tbsizes.2")) {
				profile.addCol(PROFILE_KEYS.get(type)).addCol(_allCols[1]).addCol(
						_allCols[2]).addCol(Fmt.parent(Long.valueOf(_allCols[1]), Long.valueOf(_allCols[2]))).breakRow();
			}

			// TP
			if (type.startsWith("/tbsizes.4")) {
				tp.addCol(TP_KEYS.get(type)).addCol(_allCols[1]).addCol(
						_allCols[2]).addCol(Fmt.parent(Long.valueOf(_allCols[1]), Long.valueOf(_allCols[2]))).breakRow();
			}
			// FEED
			if (type.startsWith("/tbsizes.5")) {
				tp.addCol(SIZE_DETAIL_KEYS.get(type)).addCol(_allCols[1]).addCol(
						_allCols[2]).addCol(Fmt.parent(Long.valueOf(_allCols[1]), Long.valueOf(_allCols[2]))).breakRow();
			}
			// 如何测量
			if (type.startsWith("howMeasure")) {
				link.addCol("如何测量").addCol(_allCols[1]).breakRow();
			}
			// 尺码中心
			if (type.startsWith("manageSize")) {
				link.addCol("去尺码中心").addCol(_allCols[1]).breakRow();
			}
			// sizeDetail page
			if (type.startsWith("sizeDetail")) {
				sizeDetail.addCol(SIZE_DETAIL_KEYS.get(type)).addCol(_allCols[1]).addCol(
						_allCols[2]).addCol(Fmt.parent(Long.valueOf(_allCols[1]), Long.valueOf(_allCols[2]))).breakRow();
			}

		}
		// detail.sort(Table.SORT_VALUE);
		XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
	}

	static Map<String, String> MINI_KEYS = MapUtils.asMap(("/tbsizes.1.17-"
			+ "触发按钮-" + "/tbsizes.1.20-" + "修改按钮-" + "/tbsizes.1.21-"
			+ "快速编辑 -" + "/tbsizes.1.22-" + "非空下拉-" + "/tbsizes.1.23-" + "空下拉")
			.split("-"));

	static Map<String, String> PROFILE_KEYS = MapUtils.asMap(("/tbsizes.2.1-"
			+ "快速录入（鞋子）-" + "/tbsizes.2.2-" + "快速录入（内衣）-" + "/tbsizes.2.3-"
			+ "整体身形图（右切换）-" + "/tbsizes.2.4-" + "鞋子尺码（Tab）-" + "/tbsizes.2.5-"
			+ "内衣尺码（Tab）-" + "/tbsizes.2.6-" + "整体身形（Tab）-" + "/tbsizes.2.8-"
			+ "/tbsizes.2.10-" + "大码女装推荐宝贝（暂时放这里）-"
			+ "更多身形（按钮）").split("-"));

	static Map<String, String> TP_KEYS = MapUtils.asMap(("/tbsizes.4.1-"
			+ "鞋子批量加减1-" + "/tbsizes.4.2-" + "鞋子恢复默认-" + "/tbsizes.4.3-"
			+ "鞋子测试准确性-" + "/tbsizes.4.4-" + "文胸批量加减1-" + "/tbsizes.4.5-"
			+ "文胸恢复默认-" + "/tbsizes.4.6-" + "文胸测试准确性-"
			+ "/tbsizes.4.7-" + "服装测试准确性-"
			+ "/tbsizes.4.10-" + "应用该模板-"
			+ "/tbsizes.4.8-" + "宝贝使用详情").split("-"));

	static Map<String, String> SIZE_DETAIL_KEYS = MapUtils.asMap((
			"/tbsizes.5.1-"+"相似体形-" +
			"sizeDetailGroup-"+"sizeDetailGroup页面-" +
			"sizeDetailFeed-"+"sizeFeedDetial页面-" +
			"sizeDetailAll-"+"尺码详情总页面-" +
			"sizeDetailGroup_1-"+"sizeDetailGroup页之snt=1-" +
			"sizeDetailGroup_2-"+"sizeDetailGroup页之snt=2-" +
			"sizeDetailGroup_3-"+"sizeDetailGroup页之分页-" +
			"sizeDetailGroup_4-"+"sizeDetailGroup页之尺码无异议-" +
			"sizeDetailGroup_5-"+"sizeDetailGroup页之尺码偏大-" +
			"sizeDetailGroup_6-"+"sizeDetailGroup页之尺码偏小-" +
			"sizeDetailFeed_1-"+"sizeDetailFeed页之外部重定向-" +
			"sizeDetailFeed_2-"+"sizeDetailFeed页之分页-" +
			"sizeDetailFeed_3-"+"sizeDetailFeed页之外链接-" +
			"sizeDetailOther-"+"sizeDetailGroup页之遗漏的"
			).split("-"));
}
