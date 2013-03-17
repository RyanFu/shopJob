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
 * �����ҳ����ͳ��
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

		Report report = Report.newReport("�����ҳ����ͳ�Ƽ�ҳ��PV/UV");

		Table mini = report.newViewTable("detail", "miniҳ��������");
		mini.addCol("���λ��").addCol("PV").addCol("UV_USER").addCol("PV/UV")
				.breakRow();

		Table profile = report.newViewTable("total", "��̨¼��ҳ��������");
		profile.addCol("���λ��").addCol("PV").addCol("UV_USER").addCol("PV/UV")
				.breakRow();

		Table tp = report.newViewTable("tp", "����ģ��ҳ��������");
		tp.addCol("���λ��").addCol("PV").addCol("UV_USER").addCol("PV/UV")
				.breakRow();

		Table link = report.newViewTable("link", "miniҳ��������");
		link.addCol("���λ��").addCol("PV").breakRow();


		Table sizeDetail = report.newViewTable("sizeDetail", "������������ҳ��PV/UV");
		sizeDetail.addCol("ҳ��").addCol("PV").addCol("UV_USER").addCol("PV/UV")
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
			// ��β���
			if (type.startsWith("howMeasure")) {
				link.addCol("��β���").addCol(_allCols[1]).breakRow();
			}
			// ��������
			if (type.startsWith("manageSize")) {
				link.addCol("ȥ��������").addCol(_allCols[1]).breakRow();
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
			+ "������ť-" + "/tbsizes.1.20-" + "�޸İ�ť-" + "/tbsizes.1.21-"
			+ "���ٱ༭ -" + "/tbsizes.1.22-" + "�ǿ�����-" + "/tbsizes.1.23-" + "������")
			.split("-"));

	static Map<String, String> PROFILE_KEYS = MapUtils.asMap(("/tbsizes.2.1-"
			+ "����¼�루Ь�ӣ�-" + "/tbsizes.2.2-" + "����¼�루���£�-" + "/tbsizes.2.3-"
			+ "��������ͼ�����л���-" + "/tbsizes.2.4-" + "Ь�ӳ��루Tab��-" + "/tbsizes.2.5-"
			+ "���³��루Tab��-" + "/tbsizes.2.6-" + "�������Σ�Tab��-" + "/tbsizes.2.8-"
			+ "/tbsizes.2.10-" + "����Ůװ�Ƽ���������ʱ�����-"
			+ "�������Σ���ť��").split("-"));

	static Map<String, String> TP_KEYS = MapUtils.asMap(("/tbsizes.4.1-"
			+ "Ь�������Ӽ�1-" + "/tbsizes.4.2-" + "Ь�ӻָ�Ĭ��-" + "/tbsizes.4.3-"
			+ "Ь�Ӳ���׼ȷ��-" + "/tbsizes.4.4-" + "���������Ӽ�1-" + "/tbsizes.4.5-"
			+ "���ػָ�Ĭ��-" + "/tbsizes.4.6-" + "���ز���׼ȷ��-"
			+ "/tbsizes.4.7-" + "��װ����׼ȷ��-"
			+ "/tbsizes.4.10-" + "Ӧ�ø�ģ��-"
			+ "/tbsizes.4.8-" + "����ʹ������").split("-"));

	static Map<String, String> SIZE_DETAIL_KEYS = MapUtils.asMap((
			"/tbsizes.5.1-"+"��������-" +
			"sizeDetailGroup-"+"sizeDetailGroupҳ��-" +
			"sizeDetailFeed-"+"sizeFeedDetialҳ��-" +
			"sizeDetailAll-"+"����������ҳ��-" +
			"sizeDetailGroup_1-"+"sizeDetailGroupҳ֮snt=1-" +
			"sizeDetailGroup_2-"+"sizeDetailGroupҳ֮snt=2-" +
			"sizeDetailGroup_3-"+"sizeDetailGroupҳ֮��ҳ-" +
			"sizeDetailGroup_4-"+"sizeDetailGroupҳ֮����������-" +
			"sizeDetailGroup_5-"+"sizeDetailGroupҳ֮����ƫ��-" +
			"sizeDetailGroup_6-"+"sizeDetailGroupҳ֮����ƫС-" +
			"sizeDetailFeed_1-"+"sizeDetailFeedҳ֮�ⲿ�ض���-" +
			"sizeDetailFeed_2-"+"sizeDetailFeedҳ֮��ҳ-" +
			"sizeDetailFeed_3-"+"sizeDetailFeedҳ֮������-" +
			"sizeDetailOther-"+"sizeDetailGroupҳ֮��©��"
			).split("-"));
}
