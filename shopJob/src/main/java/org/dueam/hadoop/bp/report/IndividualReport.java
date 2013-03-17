package org.dueam.hadoop.bp.report;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.dueam.hadoop.common.util.Fmt;
import org.dueam.hadoop.common.util.MapUtils;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

/**
 * 个性化list通用统计报表
 */
public class IndividualReport {
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
		String name = "";
		if (args.length > 1) {
			name = args[1];
		}

		Report report = Report.newReport(name + "(个性化list) 转化率报表");

		Map<String, List<String[]>> today = MapUtils.map(Utils.read(input),
				CTRL_A);
		Map<String, String[]> ipvMap = MapUtils.toMap(today.get("ipv"));
		Map<String, String[]> tradeMap = MapUtils.toMap(today.get("trade"));
		Table total = report.newViewTable("total", "基本详情");
		Table trade = report.newViewTable("trade", "基本详情(交易相关)");
		Table detail = report.newTable("detail", "浏览详情");

		Table async = report.newTable("asnyc", "点击统计");

		total.addCol("域名").addCol("PV").addCol("UV")
		/* .addCol("UV（MID）") */.addCol("IPV").addCol("IPV-UV")
		/* .addCol("IPV-UV（MID）") */.addCol("浏览转化率").addCol("PV/人").addCol(
				Report.BREAK_VALUE);
		trade.addCol("域名").addCol("支付宝金额").addCol("IPV-UV")
		/* .addCol("UV（MID）") */.addCol("购买UV").addCol("购买转化率")
		/* .addCol("IPV-UV（MID）") */.addCol("订单数").addCol("客单价").addCol(
				Report.BREAK_VALUE);

		long asycPV = 0;
		for (String[] cols : today.get("async")) {

			if (cols != null) {
				String value = asnycMap.get(cols[0]);
				asycPV = asycPV + Long.valueOf(cols[1]);
				if (StringUtils.isNotBlank(value)) {
					async.addCol(value + "-PV", cols[1]);
					async.addCol(value + "-UV", cols[2]);

				}

			}

		}

		for (String[] cols : today.get("ls")) {
			if (!HOSTS.contains(cols[0]))
				continue;

			// 加异步点击
			cols[1] = String.valueOf(asycPV + Long.valueOf(cols[1]));

			detail.addCol(cols[0] + "-PV", cols[1]);
			detail.addCol(cols[0] + "-UV", cols[2]);
			String[] _cols = ipvMap.get(cols[0]);
			if (_cols != null) {
				detail.addCol(cols[0] + "-IPV", _cols[0]);
				detail.addCol(cols[0] + "-IPV-UV", _cols[1]);
				detail.addCol(cols[0] + "浏览转化率(万分比)", Fmt.parent3(_cols[1],
						cols[2]));

				total.addCol(cols[0]);
				total.addCol(cols[0] + "pv", cols[1]);
				total.addCol(cols[0] + "uv", cols[2]);
				total.addCol(cols[0] + "ipv", _cols[0]);
				total.addCol(cols[0] + "ipv-uv", _cols[1]);
				total.addCol(cols[0] + "trasfer", Fmt
						.parent2(_cols[1], cols[2]));
				total.addCol(cols[0] + "avg-pv", Fmt.div(cols[1], cols[2]));
				total.addCol(Report.BREAK_VALUE);

				String[] _tradeCols = tradeMap.get(cols[0]);
				if (_tradeCols != null) {
					detail.addCol(cols[0] + "-支付宝金额", _tradeCols[0]);
					detail.addCol(cols[0] + "-购买UV", _tradeCols[2]);
					detail.addCol(cols[0] + "-订单数", _tradeCols[4]);
					detail.addCol(cols[0] + "购买转化率(万分比)", Fmt.parent3(
							_tradeCols[2], _cols[1]));

					trade.addCol(cols[0]);
					trade.addCol(cols[0] + "trade-moneny", _tradeCols[0]);
					trade.addCol(cols[0] + "trade-ipv-uv", _cols[1]);
					trade.addCol(cols[0] + "trade-buyer-uv", _tradeCols[2]);
					trade.addCol(cols[0] + "trade-trasfer", Fmt.parent2(
							_tradeCols[2], _cols[1]));
					trade.addCol(cols[0] + "trade-count", _tradeCols[4]);
					trade.addCol(cols[0] + "trade-avg-price", Fmt.div(
							_tradeCols[0], _tradeCols[2]));
					trade.addCol(Report.BREAK_VALUE);

				}
			}

		}

		XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
	}

	private static List<String> HOSTS = Arrays
			.asList(new String[] { "list.taobao.com" });

	private static Map<String, String> asnycMap = new HashMap<String, String>();

	static {

		asnycMap.put("/hesper.69.1", "类目区-类目点击");
		asnycMap.put("/hesper.69.2", "类目区-面包屑点击");
		asnycMap.put("/hesper.69.3", "类目区-更多类目");

		asnycMap.put("/hesper.63.1", "属性区-属性点击");
		asnycMap.put("/hesper.63.2", "属性区-属性多选");
		asnycMap.put("/hesper.63.3", "属性区-属性单选");
		asnycMap.put("/hesper.63.4", "属性区-更多属性");

		asnycMap.put("/hesper.67.1", "筛选区-排序点击");
		asnycMap.put("/hesper.67.2", "筛选区-价格点击");
		asnycMap.put("/hesper.67.3", "筛选区-价格区间筛选");
		asnycMap.put("/hesper.67.4", "筛选区-关键字搜索");
		asnycMap.put("/hesper.67.5", "筛选区-特殊服务");
		asnycMap.put("/hesper.67.6", "筛选区-推广服务");
		asnycMap.put("/hesper.67.7", "筛选区-合并同款");

		asnycMap.put("/hesper.68.1", "页底翻页区-上一页");
		asnycMap.put("/hesper.68.2", "页底翻页区-下一页");
		asnycMap.put("/hesper.68.3", "页底翻页区-页码点击");

		asnycMap.put("/hesper.70.1", "tms-尺码档案");
		asnycMap.put("/hesper.70.2", "tms-星座");
		asnycMap.put("/hesper.70.3", "tms-收藏店铺上新");
		asnycMap.put("/hesper.70.4", "tms-品牌推荐");
		asnycMap.put("/hesper.70.5", "tms-同地区推荐");
		asnycMap.put("/hesper.70.6", "tms-尺码换一组");

	}

}
