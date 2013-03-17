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
 * ������������ͳ��
 */
public class MysizeTp {
	private static char TAB = (char) 0x09;
	// ��һ������Ϊ�յ�
	static String[] partName = new String[] { "", "���", "����", "���", "��Χ", "��Χ", "��Χ", "�ų�", "�ſ�", "����Χ", "��Χ��" };
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

		Report report = Report.newReport("����ģ���������ͳ��");

		Table tpGroup = report.newGroupTable("tpGroup", "�����õķ�װģ��ĸ������ͳ��(ֻչʾ����50��)");

		Table countSizeSKU = report.newGroupTable("countSizeSKU", "�����õĵ�����װģ��ĳ�������ֲ�ͳ��");

		Table countTp = report.newGroupTable("countTp", "�����������õ�ģ���������");

		Table xiePart = report.newViewTable("xiePart", "Ь��ģ�������ģ�����");
		xiePart.addCol("���нſ��ģ����").addCol("Ь��ģ������").addCol("��ռ����").addCol(Report.BREAK_VALUE);


		Table tpCount = report.newGroupTable("tpCount", "����ģ�����");
		Table tpCountByType = report.newGroupTable("tpCountByType", "����ģ����������ͷ�");
		Table tpInUseCount = report.newGroupTable("tpInUseCount", "���б�ʹ�õ�ģ�����");
		Table tpInUseCountByType = report.newGroupTable("tpInUseCountByType", "���б�ʹ��ģ����������ͷ�");
		Table tpAuctionCount = report.newGroupTable("tpAuctionCount", "����ʹ��ģ���[����]����");
		Table tpAuctionCountByCat = report.newGroupTable("tpAuctionCountByCat", "����ʹ��ģ���[����]��������Ŀ��");
		Table userCountByCat = report.newGroupTable("userCountByCat", "����ʹ��ģ���[����]��������Ŀ��");




		for (String line : Utils.readWithCharset(input, "utf-8")) {
			if (line == null)
				return;
			String[] _allCols = StringUtils.splitByWholeSeparator(line, "\t");
			String[] _allCols_1 = StringUtils.splitPreserveAllTokens(line, CTRL_A);

			String type = _allCols[0];
			String type_1 = _allCols_1[0];


			if (type_1.equals("tpCount")) {
				tpCount.addCol("����ģ�����", _allCols_1[1]);
			}
			if (type_1.equals("tpCountByType")) {
				tpCountByType.addCol(convertTpType(_allCols_1[1]), _allCols_1[2]);
			}
			if (type_1.equals("tpInUseCount")) {
				tpInUseCount.addCol("���б�ʹ�õ�ģ�����", _allCols_1[1]);
			}
			if (type_1.equals("tpInUseCountByType")) {
				tpInUseCountByType.addCol(convertTpType(_allCols_1[1]), _allCols_1[2]);
			}
			if (type_1.equals("tpAuctionCount")) {
				tpAuctionCount.addCol("����ʹ��ģ��ı�������", _allCols_1[1]);
			}
//			if (type_1.equals("tpAuctionCountByCat")) {
//				tpAuctionCountByCat.addCol(convertCatName(_allCols_1[1]), _allCols_1[2]);
//			}
//			if (type_1.equals("userCountByCat")) {
//				userCountByCat.addCol(convertCatName(_allCols_1[1]), _allCols_1[2]);
//			}



			if (type.startsWith("tpGroup-")) {
				if (Integer.valueOf(_allCols[1]) >= 50)// ֻչʾ����50��
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
		//��ӪҪ�����ǵ�˳���ţ����Ե����������
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
	 * ת��key,����12:��ߡ�����
	 *
	 * @throws Exception
	 */
	public static String convertKey(String groupKey) {
		String array = groupKey.replace("tpGroup-", "");// ȥ����־��
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < array.getBytes().length; i++) {
			int pointer = Integer.valueOf(array.substring(i, i + 1));
			sb.append(partName[pointer]);
			sb.append("��");
		}

		sb.delete(sb.length() - 1, sb.length());

		return sb.toString();
	}

	/**
	 * ��Ŀ����
	 *
	 * @throws Exception
	 */
	public static String convertCatName(String catId) {
		Map<String, String> map = new HashMap<String, String>();
		map.put("16", "Ůװ/Ůʿ��Ʒ");
		map.put("30", "��װ");
		map.put("50011740", "��Ь");
		map.put("50006843", "ŮЬ");
		map.put("50012029", "�˶�Ьnew");
		map.put("50013886", "����/��ɽ/ҰӪ/������Ʒ");
		map.put("50008881", "����");
		map.put("50008883", "������װ");
		map.put("1625", "Ůʿ����/��ʿ����/�Ҿӷ�");

		if (map.get(catId) != null) {
			return map.get(catId);
		} else {
			return catId+"��Ŀ";
		}
	}


	/**
	 * ģ������
	 *
	 * @throws Exception
	 */
	public static String convertTpType(String type) {
		Map<String, String> map = new HashMap<String, String>();
		map.put("1", "��װģ��");
		map.put("2", "Ь��ģ��");
		map.put("3", "����ģ��");
		if (map.get(type) != null) {
			return map.get(type);
		} else {
			return "";
		}
	}

}
