package org.dueam.hadoop.bp.report;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

/**
 * 高级搜索的数据统计报表
 */
public class AdSearchReport {
	 private static char CTRL_A = (char) 0x01;
	    public static void main(String[] args) throws IOException {
	        String input = args[0];
	        if (!new File(input).exists()) {
	            System.out.println("File Not Exist ! => " + input);
	            return;
	        }
	        Report report = Report.newReport("高级搜索的统计报表");
	        Table totalTable = report.newTable("total", "总体数据汇总");
	        Table filterFormTable = report.newTable("filterForm", "筛选项的使用情况");
	        Table sellerTable = report.newTable("seller", "买卖家的使用情况");
	        Table referPvTable = report.newTable("referPv", "高级搜索的来源pv统计");
	        Table referUvTable = report.newTable("referUv", "高级搜索的来源uv统计");
//			Table oneWeek = report.newTable("oneWeek", "一周中用户的访问频率统计");

	        int sellerPv = 0;
	        int nickSellerPv = 0;
	        int keywordModule = 0;
	        int minCount=2;

	        for (String line : Utils.readWithCharset(input, "utf-8")) {
	            String[] _allCols = StringUtils.splitPreserveAllTokens(line, CTRL_A);

	            if (_allCols.length > 1) {
	                if (StringUtils.equals(_allCols[0], "entry")) {
	                	//访问情况
	                	totalTable.addCol("访问pv", _allCols[1]);
	                	totalTable.addCol("访问uv", _allCols[2]);
	                }else if(StringUtils.equals(_allCols[0], "noresult")){
	                	//跳到list无结果的情况
	                	totalTable.addCol("跳转到list无结果的pv", _allCols[1]);
	                }else if (StringUtils.equals(_allCols[0], "filterForm")) {
	                	//点击搜索的情况
	                	totalTable.addCol("点击搜索的pv",_allCols[1]);
	                	totalTable.addCol("点击搜索的uv",_allCols[2]);
	                	//筛选条件的使用情况
	                	filterFormTable.addCol("市场",_allCols[3]);
	                	filterFormTable.addCol("关键词",_allCols[4]);
	                	filterFormTable.addCol("昵称",_allCols[5]);
	                	filterFormTable.addCol("旺旺在线",_allCols[6]);
	                	filterFormTable.addCol("价格区间下限",_allCols[7]);
	                	filterFormTable.addCol("价格区间上限",_allCols[8]);
	                	filterFormTable.addCol("卖家信用等级",_allCols[9]);
	                	filterFormTable.addCol("消费者保障",_allCols[10]);
	                	filterFormTable.addCol("正品保障",_allCols[11]);
	                	filterFormTable.addCol("品牌授权",_allCols[12]);
	                	filterFormTable.addCol("七天退换",_allCols[13]);
	                	filterFormTable.addCol("假一赔三",_allCols[14]);
	                	filterFormTable.addCol("第三方质检",_allCols[15]);
	                	filterFormTable.addCol("商品优惠",_allCols[16]);
	                	filterFormTable.addCol("地区",_allCols[17]);
	                	filterFormTable.addCol("运费险",_allCols[18]);
	                	filterFormTable.addCol("24小时发货",_allCols[19]);
	                	filterFormTable.addCol("闪电发货",_allCols[20]);
	                	filterFormTable.addCol("二手",_allCols[21]);
	                	filterFormTable.addCol("全新",_allCols[22]);
	                	filterFormTable.addCol("货到付款",_allCols[23]);
	                	filterFormTable.addCol("信用卡",_allCols[24]);
	                	filterFormTable.addCol("公益宝贝",_allCols[25]);
	                	filterFormTable.addCol("海外商品",_allCols[26]);
	                }else if (StringUtils.equals(_allCols[0], "keyword")) {
	                	if(StringUtils.equals("1", _allCols[1])){
	                		//关键词分析模块之热门品牌
	                		totalTable.addCol("热门品牌的访问pv", _allCols[2]);
	                		totalTable.addCol("热门品牌的访问uv", _allCols[3]);
	                		keywordModule += Integer.parseInt(_allCols[2]);
	                	}else if(StringUtils.equals("2", _allCols[1])){
	                		//关键词分析模块之热门商品
	                		totalTable.addCol("热门商品的访问pv", _allCols[2]);
	                		totalTable.addCol("热门商品的访问uv", _allCols[3]);
	                		//关键词分析模块的总pv
	                		keywordModule += Integer.parseInt(_allCols[2]);
	                		totalTable.addCol("关键词分析模块的总访问pv", new Integer(keywordModule).toString());
	                	}
	                }else if (StringUtils.equals(_allCols[0], "survey")) {
	                	//问卷调查的使用情况
	                    totalTable.addCol("问卷调查的pv", _allCols[1]);
	                    totalTable.addCol("问卷调查的uv", _allCols[2]);
	                }else if(StringUtils.equals(_allCols[0], "all")){
	                	//访问高级搜索的卖家、买家情况
	                    if(StringUtils.equals(_allCols[1], "seller")){
	                    	sellerTable.addCol("访问高级搜索的卖家数", _allCols[2]);
	                    	sellerPv=Integer.parseInt(_allCols[2]);
	                    }else if(StringUtils.equals(_allCols[1], "seller_and_buyer")){
	                    	int buyerPv = Integer.parseInt(_allCols[2]) - sellerPv;
	                    	sellerTable.addCol("访问高级搜索的买家数", new Integer(buyerPv).toString());
	                    }
	                }else if(StringUtils.equals(_allCols[0], "nick")){
	                	//昵称搜索的卖家、买家情况
	                	if(StringUtils.equals(_allCols[1], "seller")){
	                		sellerTable.addCol("昵称搜索的卖家数", _allCols[2]);
	                		nickSellerPv = Integer.parseInt(_allCols[2]);
	                	}else if(StringUtils.equals(_allCols[1], "seller_and_buyer")){
	                		int nickBuyerPv = Integer.parseInt(_allCols[2]) - nickSellerPv;
	                		sellerTable.addCol("昵称搜索的买家数", new Integer(nickBuyerPv).toString());
	                	}
	                }else if(StringUtils.equals(_allCols[0],"refer")){
	                	//高级搜索来源统计
	                	if(NumberUtils.toLong(_allCols[2])>minCount){
	                		referPvTable.addCol(_allCols[1],_allCols[2]);
	                	}
	                	if(NumberUtils.toLong(_allCols[3])>minCount){
	                		referUvTable.addCol(_allCols[1], _allCols[3]);
	                	}
	                }
	            }
	        }
	        filterFormTable.sort(Table.SORT_VALUE);
	        referPvTable.sort(Table.SORT_VALUE);
	        referUvTable.sort(Table.SORT_VALUE);
	        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
	    }
}
