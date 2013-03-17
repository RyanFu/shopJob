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
 * 美妆市场统计-达人小站统计信息
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

	//具体的页面统计参数
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

		Report report = Report.newReport("达人小站相关数据统计");

		Table twoDays = report.newTable("twoDays", "2天中用户的访问频率统计");

		Table oneWeek = report.newTable("oneWeek", "一周中用户的访问频率统计");

		Table userVisit = report.newViewTable("userVisit", "用户黏性统计");

		userVisit.addCol("用户粘性周期").addCol("访问uid").addCol("最后一天的访问uid").addCol("uid所占比率").addCol("访问UV").addCol("最后一天的访问UV").addCol("uv所占比率").addCol(Report.BREAK_VALUE);

		Table article = report.newTable("article", "达人日记统计");

		//总体的统计
		Table total = report.newTable("total", "总体统计");

		//首页统计
		Table daren_index = report.newTable("daren_index", "首页统计");

		//达人list统计
		Table daren_list = report.newTable("daren_list", "达人list统计");

		//日记list统计
		Table diary_list = report.newTable("diary_list", "日记list统计");

		//日记detail统计
		Table article_detail = report.newTable("article_detail", "日记detail统计");

		//个人首页统计
		Table personal_home_page = report.newTable("personal_home_page", "个人首页统计");

		//专辑统计
		Table personal_album = report.newTable("personal_album", "专辑统计");

		String lastDay_uid = null;//最后一天的访问uid
		String lastDay_uv = null;//最后一天的访问UV
		String article_sum = null;//文章数

		Map<Integer,Long> sum2Map = new TreeMap<Integer,Long>();
		Map<Integer,Long> sum7Map = new TreeMap<Integer,Long>();

		for (String line : Utils.readWithCharset(input, "utf-8")) {
			if(line == null) return;
			String[] allCols = StringUtils.splitPreserveAllTokens(line, CTRL_A);
			if (StringUtils.equals(TOTAL,allCols[0])) {
				lastDay_uid = allCols[4];
				lastDay_uv = allCols[2];
				total.addCol("总PV",allCols[1]);
				total.addCol("总UV",allCols[2]);
				total.addCol("人均浏览页",Fmt.div(allCols[1], allCols[2]));
				total.addCol("会员PV",allCols[3]);
				total.addCol("会员UV",allCols[4]);
				total.addCol("会员人均浏览页",Fmt.div(allCols[3], allCols[4]));
            }
            if (StringUtils.equals(TWODAYS,allCols[0])) {
            	if(StringUtils.equals("uid",allCols[1])){
            		//twoDays.addCol("访问频率"+allCols[2] + "天-uid", allCols[3]);
            		if(StringUtils.equals(allCols[2],"2")){
            			userVisit.addCol("2天").addCol(allCols[3]).addCol(lastDay_uid).addCol(Fmt.parent2(allCols[3], lastDay_uid));
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
            		//oneWeek.addCol("访问频率"+allCols[2] + "天-uid", allCols[3]);
            		if(StringUtils.equals(allCols[2],"7")){
            			userVisit.addCol("一周").addCol(allCols[3]).addCol(lastDay_uid).addCol(Fmt.parent2(allCols[3], lastDay_uid));
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
            	article.addCol("新增日记", allCols[1]);
            }
            if (StringUtils.equals(MODIFY_ARTICLE,allCols[0])) {
            	article.addCol("修改日记", allCols[1]);
            }
            if (StringUtils.equals(TOTAL_ARTICLE,allCols[0])) {
            	article.addCol("总日记数", allCols[1]);
            	article_sum = allCols[1];
            }
            if(StringUtils.equals(DAREN_INDEX,allCols[0])){
            	daren_index.addCol("总PV",allCols[1]);
            	daren_index.addCol("总UV",allCols[2]);
            	daren_index.addCol("人均浏览页",Fmt.div(allCols[1], allCols[2]));
            }else if(StringUtils.equals(DAREN_LIST,allCols[0])){
            	daren_list.addCol("总PV",allCols[1]);
            	daren_list.addCol("总UV",allCols[2]);
            	daren_list.addCol("人均浏览页",Fmt.div(allCols[1], allCols[2]));
            }else if(StringUtils.equals(DIARY_LIST,allCols[0])){
            	diary_list.addCol("总PV",allCols[1]);
            	diary_list.addCol("总UV",allCols[2]);
            	diary_list.addCol("人均浏览页",Fmt.div(allCols[1], allCols[2]));
            }else if(StringUtils.equals(ARTICLE_DETAIL,allCols[0])){
            	article_detail.addCol("总PV",allCols[1]);
            	article_detail.addCol("总UV",allCols[2]);
            	article_detail.addCol("人均浏览页",Fmt.div(allCols[1], allCols[2]));
            }else if(StringUtils.equals(PERSONAL_HOME_PAGE,allCols[0])){
            	personal_home_page.addCol("总PV",allCols[1]);
            	personal_home_page.addCol("总UV",allCols[2]);
            	personal_home_page.addCol("人均浏览页",Fmt.div(allCols[1], allCols[2]));
            }else if(StringUtils.equals(PERSONAL_ALBUM,allCols[0])){
            	personal_album.addCol("总PV",allCols[1]);
            	personal_album.addCol("总UV",allCols[2]);
            	personal_album.addCol("人均浏览页",Fmt.div(allCols[1], allCols[2]));
            }
		}
		for(int i = 0;i<sum2Map.size();i++){
			int day = i+1;
			twoDays.addCol("访问频率"+day + "天-uv", sum2Map.get(i+1).toString());
		}
		for(int i = 0;i<sum7Map.size();i++){
			int day = i+1;
			oneWeek.addCol("访问频率"+day + "天-uv", sum7Map.get(i+1).toString());
		}
		total.addCol("文章数",article_sum);
		twoDays.sort(Table.SORT_KEY);
		oneWeek.sort(Table.SORT_KEY);
		XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
	}

}
