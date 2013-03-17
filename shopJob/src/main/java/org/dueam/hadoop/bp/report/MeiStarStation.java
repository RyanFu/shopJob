package org.dueam.hadoop.bp.report;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;
import org.dueam.hadoop.common.util.Fmt;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

/**
 * ��ױ�г�ͳ��-����Сվͳ����Ϣ
 */
public class MeiStarStation {
	private static char TAB = (char) 0x09;
	private static char CTRL_A = (char) 0x01;
	private static String TWODAYS = "day2";
	private static String ONE_WEEK = "day7";
	private static String TOTAL = "total";
	private static String ADD_ARTICLE = "addArticle";
	private static String MODIFY_ARTICLE = "modifyArticle";
	private static String TOTAL_ARTICLE = "totalArticle";

	//�����ҳ��ͳ�Ʋ���
	private static String DAREN_INDEX = "daren_index";
	private static String DAREN_LIST = "daren_list";
	private static String DIARY_LIST = "diary_list";
	private static String ARTICLE_DETAIL = "article_detail";
	private static String PERSONAL_HOME_PAGE = "personal_home_page";
	private static String PERSONAL_ALBUM = "personal_album";

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

		Report report = Report.newReport("����Сվ�������ͳ��");

		Table twoDays = report.newTable("twoDays", "2�����û��ķ���Ƶ��ͳ��");

		Table oneWeek = report.newTable("oneWeek", "һ�����û��ķ���Ƶ��ͳ��");

		Table userVisit = report.newViewTable("userVisit", "�û����ͳ��");

		userVisit.addCol("�û�ճ������").addCol("����uid").addCol("���һ��ķ���uid").addCol("uid��ռ����").addCol("����UV").addCol("���һ��ķ���UV").addCol("uv��ռ����").addCol(Report.BREAK_VALUE);

		Table article = report.newTable("article", "�����ռ�ͳ��");

		//�����ͳ��
		Table total = report.newTable("total", "����ͳ��");

		//��ҳͳ��
		Table daren_index = report.newTable("daren_index", "��ҳͳ��");

		//����listͳ��
		Table daren_list = report.newTable("daren_list", "����listͳ��");

		//�ռ�listͳ��
		Table diary_list = report.newTable("diary_list", "�ռ�listͳ��");

		//�ռ�detailͳ��
		Table article_detail = report.newTable("article_detail", "�ռ�detailͳ��");

		//������ҳͳ��
		Table personal_home_page = report.newTable("personal_home_page", "������ҳͳ��");

		//ר��ͳ��
		Table personal_album = report.newTable("personal_album", "ר��ͳ��");

		String lastDay_uid = null;//���һ��ķ���uid
		String lastDay_uv = null;//���һ��ķ���UV
		String article_sum = null;//������

		Map<Integer,Long> sum2Map = new TreeMap<Integer,Long>();
		Map<Integer,Long> sum7Map = new TreeMap<Integer,Long>();

		for (String line : Utils.readWithCharset(input, "utf-8")) {
			if(line == null) return;
			String[] allCols = StringUtils.splitPreserveAllTokens(line, CTRL_A);
			if (StringUtils.equals(TOTAL,allCols[0])) {
				lastDay_uid = allCols[4];
				lastDay_uv = allCols[2];
				total.addCol("��PV",allCols[1]);
				total.addCol("��UV",allCols[2]);
				total.addCol("�˾����ҳ",Fmt.div(allCols[1], allCols[2]));
				total.addCol("��ԱPV",allCols[3]);
				total.addCol("��ԱUV",allCols[4]);
				total.addCol("��Ա�˾����ҳ",Fmt.div(allCols[3], allCols[4]));
            }
            if (StringUtils.equals(TWODAYS,allCols[0])) {
            	if(StringUtils.equals("uid",allCols[1])){
            		//twoDays.addCol("����Ƶ��"+allCols[2] + "��-uid", allCols[3]);
            		if(StringUtils.equals(allCols[2],"2")){
            			userVisit.addCol("2��").addCol(allCols[3]).addCol(lastDay_uid).addCol(Fmt.parent2(allCols[3], lastDay_uid));
            		}
            	}else if(StringUtils.equals("uv",allCols[1])){
            		int visitDay = Integer.parseInt(allCols[2]);
            		long sum = Long.parseLong(allCols[3]);
            		if(sum2Map.size()>0){
                		Iterator iterator = sum2Map.keySet().iterator();
            			while(iterator.hasNext()){
            				int key = (Integer)iterator.next();
            				if(key < visitDay){
            					sum2Map.put(key,(Long)sum2Map.get(key)+ sum);
            				}
            			}
            		}
            		if(sum2Map.get(visitDay)==null){
            			sum2Map.put(visitDay, sum);
            		}

            		if(StringUtils.equals(allCols[2],"2")){
            			userVisit.addCol(allCols[3]).addCol(lastDay_uv).addCol(Fmt.parent2(allCols[3], lastDay_uv)).addCol(Report.BREAK_VALUE);
            		}
            	}
            }
            if (StringUtils.equals(ONE_WEEK,allCols[0])) {
            	if(StringUtils.equals("uid",allCols[1])){
            		//oneWeek.addCol("����Ƶ��"+allCols[2] + "��-uid", allCols[3]);
            		if(StringUtils.equals(allCols[2],"7")){
            			userVisit.addCol("һ��").addCol(allCols[3]).addCol(lastDay_uid).addCol(Fmt.parent2(allCols[3], lastDay_uid));
            		}
            	}else if(StringUtils.equals("uv",allCols[1])){
            		int visitDay = Integer.parseInt(allCols[2]);
            		long sum = Long.parseLong(allCols[3]);
            		if(sum7Map.size()>0){
                		Iterator iterator = sum7Map.keySet().iterator();
            			while(iterator.hasNext()){
            				int key = (Integer)iterator.next();
            				if(key < visitDay){
            					sum7Map.put(key,(Long)sum7Map.get(key)+ sum);
            				}
            			}
            		}
            		if(sum7Map.get(visitDay)==null){
            			sum7Map.put(visitDay, sum);
            		}
            		if(StringUtils.equals(allCols[2],"7")){
            			userVisit.addCol(allCols[3]).addCol(lastDay_uv).addCol(Fmt.parent2(allCols[3], lastDay_uv)).addCol(Report.BREAK_VALUE);
            		}
            	}
            }
            if (StringUtils.equals(ADD_ARTICLE,allCols[0])) {
            	article.addCol("�����ռ�", allCols[1]);
            }
            if (StringUtils.equals(MODIFY_ARTICLE,allCols[0])) {
            	article.addCol("�޸��ռ�", allCols[1]);
            }
            if (StringUtils.equals(TOTAL_ARTICLE,allCols[0])) {
            	article.addCol("���ռ���", allCols[1]);
            	article_sum = allCols[1];
            }
            if(StringUtils.equals(DAREN_INDEX,allCols[0])){
            	daren_index.addCol("��PV",allCols[1]);
            	daren_index.addCol("��UV",allCols[2]);
            	daren_index.addCol("�˾����ҳ",Fmt.div(allCols[1], allCols[2]));
            }else if(StringUtils.equals(DAREN_LIST,allCols[0])){
            	daren_list.addCol("��PV",allCols[1]);
            	daren_list.addCol("��UV",allCols[2]);
            	daren_list.addCol("�˾����ҳ",Fmt.div(allCols[1], allCols[2]));
            }else if(StringUtils.equals(DIARY_LIST,allCols[0])){
            	diary_list.addCol("��PV",allCols[1]);
            	diary_list.addCol("��UV",allCols[2]);
            	diary_list.addCol("�˾����ҳ",Fmt.div(allCols[1], allCols[2]));
            }else if(StringUtils.equals(ARTICLE_DETAIL,allCols[0])){
            	article_detail.addCol("��PV",allCols[1]);
            	article_detail.addCol("��UV",allCols[2]);
            	article_detail.addCol("�˾����ҳ",Fmt.div(allCols[1], allCols[2]));
            }else if(StringUtils.equals(PERSONAL_HOME_PAGE,allCols[0])){
            	personal_home_page.addCol("��PV",allCols[1]);
            	personal_home_page.addCol("��UV",allCols[2]);
            	personal_home_page.addCol("�˾����ҳ",Fmt.div(allCols[1], allCols[2]));
            }else if(StringUtils.equals(PERSONAL_ALBUM,allCols[0])){
            	personal_album.addCol("��PV",allCols[1]);
            	personal_album.addCol("��UV",allCols[2]);
            	personal_album.addCol("�˾����ҳ",Fmt.div(allCols[1], allCols[2]));
            }
		}
		for(int i = 0;i<sum2Map.size();i++){
			int day = i+1;
			twoDays.addCol("����Ƶ��"+day + "��-uv", sum2Map.get(i+1).toString());
		}
		for(int i = 0;i<sum7Map.size();i++){
			int day = i+1;
			oneWeek.addCol("����Ƶ��"+day + "��-uv", sum7Map.get(i+1).toString());
		}
		total.addCol("������",article_sum);
		twoDays.sort(Table.SORT_KEY);
		oneWeek.sort(Table.SORT_KEY);
		XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
	}

}
