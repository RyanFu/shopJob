package org.dueam.hadoop.bp.report;
/**
 * 统计标签效果数据
 */
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

/**
 * taggou 标签效果统计
 */
public class TagGouTag {
	private static String CTRL_A = "\t";

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

		Report report = Report.newReport("标签效果统计");

		Table tag = report.newViewTable("detail", "标签效果统计");
		tag.addCol("标签").addCol("搜索PV").addCol("搜索UV").addCol("点击商品数").addCol("购物车商品数").addCol("收藏商品数").addCol("立即购买商品数").addCol("类目ID").addCol("立即购买UV").addCol("点击商品的UV")
				.addCol("IPV").addCol("欲购买转化").breakRow();

		for (String line : Utils.readWithCharset(input, "utf-8")) {
			if (line == null)
				return;
			String[] _allCols = StringUtils.splitPreserveAllTokens(line, CTRL_A);
			tag.addCol(_allCols[0]);
			tag.addCol(_allCols[0]+"s_pv", _allCols[1]);
			tag.addCol(_allCols[0]+"s_uv",_allCols[2]);
			tag.addCol(_allCols[0]+"clik_item_ct",_allCols[3]);
			tag.addCol(_allCols[0]+"gwc_item_ct",_allCols[4]);
			tag.addCol(_allCols[0]+"scj_item_ct",_allCols[5]);
			tag.addCol(_allCols[0]+"ljgm_item_ct",_allCols[6]);
			tag.addCol(_allCols[7]);
			tag.addCol(_allCols[0]+"ljgm_uv",_allCols[8]);
			tag.addCol(_allCols[0]+"ipv_uv",_allCols[9]);
			tag.addCol(_allCols[0]+"ipv",_allCols[10]);
			tag.addCol(_allCols[0]+"yg_rate",getRate(_allCols[8], _allCols[9]));
			tag.breakRow();

		}
		// detail.sort(Table.SORT_VALUE);
		XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
	}

	static String getRate(String ljgmUV, String ipvUV) {

		int a = 0;
		int b = 0;
		try {
			a = Integer.valueOf(ljgmUV);
			b = Integer.valueOf(ipvUV);
		} catch (NumberFormatException e) {
			return "";
		}

		double tmp = ((a * 1.0) / b) * 100;
		return String.format("%.2f", tmp);
	}

}
