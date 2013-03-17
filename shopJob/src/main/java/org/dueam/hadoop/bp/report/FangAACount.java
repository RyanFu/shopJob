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
		//String input = "C:\\Users\\longjia.zt\\Documents\\20120222"; // �ļ�·��
		String mainName = "�Ա�����"; // ��������
		String input = args[0]; // �ļ�·��
		// String mainName = args[1]; //��������

		if (!new File(input).exists()) {
			System.out.println("File Not Exist ! => " + input);
			return;
		}

		String AUCTIONSTATUS = "auction_status"; // ͷ����
		Report report = Report.newReport(mainName + "��Ʒ����");
		Table allAuctionByStatus = report.newGroupTable("auction_status",
				"�Ա�������Ʒ����ͳ��", "γ��", "����");
		// ����Ŀ��Ʒ��Ŀ
		String LEIMU = "leimu"; // ͷ����
		Table allAuctionByLeimu = report.newGroupTable("leimu", "����Ŀͳ����Ʒ����",
				"γ��", "����");
		// ����Ŀͳ��shang����Ʒ��Ŀ
				String MEILUONLINE = "meiluonline"; // ͷ����
				Table allAuctionByLeimuOnline = report.newGroupTable("meiluonline",
						"����Ŀͳ���ϼ���Ʒ����", "γ��", "����");
		// ����Ŀͳ���¼���Ʒ��Ŀ
		String MEILUDOWN = "meiludown"; // ͷ����
		Table allAuctionByLeimuDown = report.newGroupTable("meiludown",
				"����Ŀͳ���¼���Ʒ����", "γ��", "����");
		
		// ������ͳ��
		String CITY = "city"; // ͷ����
		Table allAuctionByCity = report.newGroupTable("city", "������ͳ���ܵ���Ʒ����",
				"γ��", "����");
		// ������ͳ��������Ʒ��Ŀ
		String CITYONLINE = "cityonline"; // ͷ����
		Table allAuctionByCityOnline = report.newGroupTable("cityonline",
				"������ͳ��������Ʒ����", "γ��", "����");

		// ������ͳ���¼���Ʒ��Ŀ
		String CITYDOWNLINE = "citydownline"; // ͷ����
		Table allAuctionByCityDownline = report.newGroupTable("citydownline",
				"������ͳ���¼���Ʒ����", "γ��", "����");
		// ������ͳ����Ʒ��Ŀ
		String SELLER = "seller"; // ͷ����
		Table allAuctionBySeller = report.newGroupTable("seller", "������ͳ����Ʒ����",
				"γ��", "����");

		// ������ͳ����Ʒ��Ŀ
		String SELLERONLINE = "selleronline"; // ͷ����
		Table allAuctionBySellerOnline = report.newGroupTable("selleronline",
				"������ͳ��������Ʒ����", "γ��", "����");

		List<String> lines = Utils.readWithCharset(input, "utf-8");
		// ��ѭ����
		if (lines != null && lines.size() > 0) {
			long allOnlineAuctions = 0;// ����������Ʒ
			long allNotOnLineAuctions = 0;// �����¼ܵ���Ʒ
			long ccAuctions = 0;
			
			for (String line : lines) {
				String[] _cols = StringUtils.splitPreserveAllTokens(line,
						CTRL_A);
				if (AUCTIONSTATUS.equals(_cols[0])) {// ˵������Ʒ���� ����״̬ά�ȵ�
					if (_cols[1].equals("0") || _cols[1].equals("1")) {
						allOnlineAuctions = allOnlineAuctions
								+ Long.parseLong(_cols[2]);
					} else if (_cols[1].equals("-2") || _cols[1].equals("-3")) {
						allNotOnLineAuctions = allNotOnLineAuctions
								+ Long.parseLong(_cols[2]);
					} else if (_cols[1].equals("-9")) {
						ccAuctions = ccAuctions + Long.parseLong(_cols[2]);
					}

				}// ����״̬ά����ɣ�
					// -------------------------���ݳ���ά��
				else if (CITY.equals(_cols[0])) {//
					allAuctionByCity
							.addCol(Report.newValue(_cols[1], _cols[2]));

				}//
					// -------------------------���ݳ���ά��ͬʱ������Ʒ��Ŀ
				else if (CITYDOWNLINE.equals(_cols[0])) {//
					allAuctionByCityDownline.addCol(Report.newValue(_cols[1],
							_cols[2]));

				}//
					// -------------------------���ݳ���ά��ͬʱ�¼���Ʒ��Ŀ
				else if (CITYONLINE.equals(_cols[0])) {//
					allAuctionByCityOnline.addCol(Report.newValue(_cols[1],
							_cols[2]));

				}//
					// -------------------------������Ŀά��ͳ������
				else if (LEIMU.equals(_cols[0])) {//
					allAuctionByLeimu.addCol(Report.newValue(
							getLeiMuNale(_cols[1]), _cols[2]));

				}//

				// -------------------------������Ŀ�¼�ά��ͳ������
				else if (MEILUDOWN.equals(_cols[0])) {//
					allAuctionByLeimuDown.addCol(Report.newValue(
							getLeiMuNale(_cols[1]), _cols[2]));

				}//
					// -------------------------������Ŀ�ϼ�ά��ͳ������
				else if (MEILUONLINE.equals(_cols[0])) {//
					allAuctionByLeimuOnline.addCol(Report.newValue(
							getLeiMuNale(_cols[1]), _cols[2]));

				}//

				// -------------------------�����û�ÿ���û�����Ŀ
				else if (SELLER.equals(_cols[0])) {//
					allAuctionBySeller.addCol(Report.newValue(_cols[1],
							_cols[2]));

				}//

				// -------------------------�����û����ߵ���Ŀ
				else if (SELLERONLINE.equals(_cols[0])) {//
					allAuctionBySellerOnline.addCol(Report.newValue(_cols[1],
							_cols[2]));

				}//

			}// for

			allAuctionByStatus.addCol(Report.newValue(
					"����",
					String.valueOf(allOnlineAuctions + allNotOnLineAuctions
							+ ccAuctions)));
			allAuctionByStatus.addCol(Report.newValue("������Ʒ����",
					String.valueOf(allOnlineAuctions)));
			allAuctionByStatus.addCol(Report.newValue("�¼���Ʒ����",
					String.valueOf(allNotOnLineAuctions)));
			allAuctionByStatus.addCol(Report.newValue("CC����",
					String.valueOf(ccAuctions)));
		}// if
		XmlReportFactory.dump(report, new FileOutputStream(input + ".xml"));

	}

	public static String getLeiMuNale(String string) {
		
		if("50023902".equals(string.trim())){
			return "������������";
		}
		else if("50023579".equals(string.trim())){
			return "ί�з���/�н����";
		}
		else if("50023597".equals(string.trim())){
			return "�ⷿ/����/����/������� ";
		}
		else if("50023598".equals(string.trim())){
			return "���ַ�/����/����";
		}
		else if("50023944".equals(string.trim())){
			return "�·�/��¥��/һ�ַ�";
		}
		else if("50026396".equals(string.trim())){
			return "��ֱ�г���Ŀ";
		}
		return "��Ŀδ֪";
		
		 
		
		
		
		
		
	}
}
