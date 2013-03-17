package org.dueam.hadoop.bp.report;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import org.dueam.hadoop.common.util.Fmt;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;


/**
 * ��ױdetailҳ���첽ͳ������
 */
public class DetailSync {
	private static char CTRL_A = (char) 0x01;

	private static Map<String, String> name = new HashMap<String, String>(20);

	private static Set<String> auctionSet = new HashSet<String>(8);

	private static Set<String> xindeSet = new HashSet<String>(8);

	static {
		name.put("qbxd", "�ڱ�ȫ����������tab");
		name.put("jcxd", "�ڱ����ʵ�������tab");
		name.put("recommentGoods", "�����Ƽ�һ��tab");
		name.put("usexd", "�ڱ�����һ��tab");
		name.put("qsearch", "�����Ƽ�һ��tab�ؼ�������");
		name.put("brandsearch", "�����Ƽ�һ��tabƷ����Ȩ����");
		name.put("globalsearch", "�����Ƽ�һ��tabȫ������");
		name.put("comentother", "�ɹ��������˵���");
		name.put("putcoment", "����spu���˵���");

		auctionSet.addAll(Arrays.asList(new String[]{"recommentGoods","brandsearch","globalsearch","qsearch"}));

		xindeSet.addAll(Arrays.asList(new String[]{"qbxd","jcxd","usexd","comentother","putcoment"}));

	}

	/**
	 * detailPagejcxd645
       detailPagerecommentGoods443616522260
       detailPageqbxd434
       detailPagebrandsearch181315
       detailPageusexd532209293
       detailPageglobalsearch211113
       detailPageputcoment711
       detailPagecomentother711
       detailPageqsearch322
	 *
	 * @param args
	 * @throws java.io.IOException
	 */
	public static void main(String[] args) throws IOException {

//		List<String[]> linkedList = new  LinkedList<String[]>();
//		for (int i = 0; i < 10; i++) {
//			linkedList.add(new String[]{"test"+i});
//		}
//		System.out.println(linkedList.size());
//		linkedList.add(0, new String[]{"test"});
//		System.out.println(linkedList.size());
//		System.out.println(linkedList.get(0));

		//String input = "F:\\devTool\\SecureCRT\\download\\20120702"; // �ļ�·��

		String input = args[0];
		if (!new File(input).exists()) {
			System.out.println("File Not Exist ! => " + input);
			return;
		}


		Report report = Report.newReport("��ױdetailҳ���첽ͳ������");
		Table table2 = report.newViewTable("summay2", "�û�������Ϊ��������");
		table2.addCol("��Ϊ").addCol("PV").addCol("UV(��Ա)").addCol("PV/��").addCol(Report.BREAK_VALUE);
		Table table = report.newTable("summay", "�û�������Ϊ����");



		List<String[]> auctionData = new ArrayList<String[]>(20);
		List<String[]> xindeData = new ArrayList<String[]>(20);
		//xindeData.add(null);
		Map<String,String[]> xindeMap = new HashMap<String,String[]>();

		for (String line : Utils.readWithCharset(input, "utf-8")) {
			String[] _allCols = StringUtils
					.splitPreserveAllTokens(line, CTRL_A);

			//-------------------��������--------------------//
            if("recommentGoods".equalsIgnoreCase(_allCols[1])){
            	auctionData.add(_allCols);
            }
            if("brandsearch".equalsIgnoreCase(_allCols[1])){
            	auctionData.add(_allCols);
            }
            if("globalsearch".equalsIgnoreCase(_allCols[1])){
            	auctionData.add(_allCols);
            }
            if("qsearch".equalsIgnoreCase(_allCols[1])){
            	auctionData.add(_allCols);
            }
            //--------------------�ĵ�����-------------------//
            if("jcxd".equalsIgnoreCase(_allCols[1])){
            	//xindeData.add(_allCols);
            	xindeMap.put("jcxd", _allCols);
            }
            if("qbxd".equalsIgnoreCase(_allCols[1])){
            	xindeMap.put("qbxd", _allCols);
            }
            if("usexd".equalsIgnoreCase(_allCols[1])){
            	xindeMap.put("usexd", _allCols);
            }
            if("putcoment".equalsIgnoreCase(_allCols[1])){
            	xindeMap.put("putcoment", _allCols);
            }
            if("comentother".equalsIgnoreCase(_allCols[1])){
            	xindeMap.put("comentother", _allCols);
            }
		}


		for (int i = 0; i < auctionData.size(); i++) {
			String preSufix = name.get(auctionData.get(i)[1]);
			if(preSufix!=null&&auctionSet.contains(auctionData.get(i)[1])){
				table.addCol(preSufix + "-PV", auctionData.get(i)[2]);
				table.addCol(preSufix + "-UV(��Ա)", (auctionData.get(i)[3]));
				table.addCol(preSufix + "-UV(MID)", (auctionData.get(i)[4]));
				table2.addCol(preSufix).addCol(auctionData.get(i)[2]).addCol((auctionData.get(i)[3])).addCol(Fmt.div(auctionData.get(i)[2], (auctionData.get(i)[4]))).breakRow();
				auctionSet.remove(auctionData.get(i)[1]);
				//linkedList.add(auctionData.get(i));
			}
		}

		if(!auctionSet.isEmpty()){
			for(String left:auctionSet){
				table.addCol(name.get(left) + "-PV", "0");
				table.addCol(name.get(left) + "-UV", "0");
				table2.addCol(name.get(left)).addCol("0").addCol("0").addCol("0").breakRow();
			}
		}

		//----------------�����ĵ�------------------//

		if(xindeMap.get("usexd") != null){
			xindeData.add(xindeMap.get("usexd"));
		}

		if(xindeMap.get("qbxd") != null){
			xindeData.add(xindeMap.get("qbxd"));
		}

		if(xindeMap.get("jcxd") != null){
			xindeData.add(xindeMap.get("jcxd"));
		}

		if(xindeMap.get("putcoment") != null){
			xindeData.add(xindeMap.get("putcoment"));
		}
		if(xindeMap.get("comentother") != null){

			xindeData.add(xindeMap.get("comentother"));
		}



		for (int i = 0; i < xindeData.size(); i++) {
			//String preSufix = name.get(xindeData.get(i)[1]);
			if(xindeSet.contains(xindeData.get(i)[1])){
				String preSufix = name.get(xindeData.get(i)[1]);
				table.addCol(preSufix + "-PV", xindeData.get(i)[2]);
				if(preSufix!=null&&("putcoment".equals(preSufix) || "comentother".equals(preSufix)) ){
					table.addCol(preSufix + "-UV(��Ա)", (xindeData.get(i)[3]));
					table2.addCol(preSufix).addCol(xindeData.get(i)[2]).addCol((xindeData.get(i)[3])).addCol(Fmt.div(xindeData.get(i)[2], (xindeData.get(i)[3]))).breakRow();
				}else{
					table.addCol(preSufix + "-UV(��Ա)", (xindeData.get(i)[3]));
					table.addCol(preSufix + "-UV(MID)", (xindeData.get(i)[4]));
					table2.addCol(preSufix).addCol(xindeData.get(i)[2]).addCol((xindeData.get(i)[3])).addCol(Fmt.div(xindeData.get(i)[2], (xindeData.get(i)[4]))).breakRow();
				}

				xindeSet.remove(xindeData.get(i)[1]);

			}
		}



		if(!xindeSet.isEmpty()){
			for(String left:xindeSet){
				table.addCol(name.get(left) + "-PV", "0");
				table.addCol(name.get(left) + "-UV", "0");
				table2.addCol(name.get(left)).addCol("0").addCol("0").addCol("0").breakRow();
			}
		}


		XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
		//XmlReportFactory.dump(report, new FileOutputStream(input+ ".xml"));
	}
}
