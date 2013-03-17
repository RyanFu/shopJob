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
 * ���Ի�listͨ��ͳ�Ʊ���
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

		Report report = Report.newReport(name + "(���Ի�list) ת���ʱ���");

		Map<String, List<String[]>> today = MapUtils.map(Utils.read(input),
				CTRL_A);
		Map<String, String[]> ipvMap = MapUtils.toMap(today.get("ipv"));
		Map<String, String[]> tradeMap = MapUtils.toMap(today.get("trade"));
		Table total = report.newViewTable("total", "��������");
		Table trade = report.newViewTable("trade", "��������(�������)");
		Table detail = report.newTable("detail", "�������");

		Table async = report.newTable("asnyc", "���ͳ��");

		total.addCol("����").addCol("PV").addCol("UV")
		/* .addCol("UV��MID��") */.addCol("IPV").addCol("IPV-UV")
		/* .addCol("IPV-UV��MID��") */.addCol("���ת����").addCol("PV/��").addCol(
				Report.BREAK_VALUE);
		trade.addCol("����").addCol("֧�������").addCol("IPV-UV")
		/* .addCol("UV��MID��") */.addCol("����UV").addCol("����ת����")
		/* .addCol("IPV-UV��MID��") */.addCol("������").addCol("�͵���").addCol(
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

			// ���첽���
			cols[1] = String.valueOf(asycPV + Long.valueOf(cols[1]));

			detail.addCol(cols[0] + "-PV", cols[1]);
			detail.addCol(cols[0] + "-UV", cols[2]);
			String[] _cols = ipvMap.get(cols[0]);
			if (_cols != null) {
				detail.addCol(cols[0] + "-IPV", _cols[0]);
				detail.addCol(cols[0] + "-IPV-UV", _cols[1]);
				detail.addCol(cols[0] + "���ת����(��ֱ�)", Fmt.parent3(_cols[1],
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
					detail.addCol(cols[0] + "-֧�������", _tradeCols[0]);
					detail.addCol(cols[0] + "-����UV", _tradeCols[2]);
					detail.addCol(cols[0] + "-������", _tradeCols[4]);
					detail.addCol(cols[0] + "����ת����(��ֱ�)", Fmt.parent3(
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

		asnycMap.put("/hesper.69.1", "��Ŀ��-��Ŀ���");
		asnycMap.put("/hesper.69.2", "��Ŀ��-���м���");
		asnycMap.put("/hesper.69.3", "��Ŀ��-������Ŀ");

		asnycMap.put("/hesper.63.1", "������-���Ե��");
		asnycMap.put("/hesper.63.2", "������-���Զ�ѡ");
		asnycMap.put("/hesper.63.3", "������-���Ե�ѡ");
		asnycMap.put("/hesper.63.4", "������-��������");

		asnycMap.put("/hesper.67.1", "ɸѡ��-������");
		asnycMap.put("/hesper.67.2", "ɸѡ��-�۸���");
		asnycMap.put("/hesper.67.3", "ɸѡ��-�۸�����ɸѡ");
		asnycMap.put("/hesper.67.4", "ɸѡ��-�ؼ�������");
		asnycMap.put("/hesper.67.5", "ɸѡ��-�������");
		asnycMap.put("/hesper.67.6", "ɸѡ��-�ƹ����");
		asnycMap.put("/hesper.67.7", "ɸѡ��-�ϲ�ͬ��");

		asnycMap.put("/hesper.68.1", "ҳ�׷�ҳ��-��һҳ");
		asnycMap.put("/hesper.68.2", "ҳ�׷�ҳ��-��һҳ");
		asnycMap.put("/hesper.68.3", "ҳ�׷�ҳ��-ҳ����");

		asnycMap.put("/hesper.70.1", "tms-���뵵��");
		asnycMap.put("/hesper.70.2", "tms-����");
		asnycMap.put("/hesper.70.3", "tms-�ղص�������");
		asnycMap.put("/hesper.70.4", "tms-Ʒ���Ƽ�");
		asnycMap.put("/hesper.70.5", "tms-ͬ�����Ƽ�");
		asnycMap.put("/hesper.70.6", "tms-���뻻һ��");

	}

}
