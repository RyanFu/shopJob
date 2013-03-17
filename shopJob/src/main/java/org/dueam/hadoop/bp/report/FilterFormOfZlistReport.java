package org.dueam.hadoop.bp.report;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

/**
 * list2.0筛选项的使用情况统计报表
 */
public class FilterFormOfZlistReport {
	private static char CTRL_A = (char) 0x01;
	public static final Map<String,String> filterFormMap = new HashMap<String, String>();
	static{
		filterFormMap.put("xie3bao=1","鞋类三包");
		filterFormMap.put("promoted_service8=tag","闪电发货");
		filterFormMap.put("ps32=32","24小时发货");
		filterFormMap.put("promoted_service16=16","30天维修");
		filterFormMap.put("promoted_service4=4","7天退换");
		filterFormMap.put("auto_post=1","自动发货");
		filterFormMap.put("ptdc=1","平台代充");
		filterFormMap.put("appointment=1","预约信息");
		filterFormMap.put("bsq=1","品牌授权");
		filterFormMap.put("is3C=1","电器城");
		filterFormMap.put("support_xcard=1","信用卡");
		filterFormMap.put("support_cod=1","货到付款");
		filterFormMap.put("combo=1","搭配减价");
		filterFormMap.put("onsale_bonusid=1:101;2:103","抵价券");
		filterFormMap.put("deli_setup=1","配送安装服务");
		filterFormMap.put("promoted_service2=tag","假一赔三");
		filterFormMap.put("yfxian=1","运费险");
		filterFormMap.put("limitPromotion=true","秒杀");
		filterFormMap.put("is_oncharity=1:102","公益宝贝");
		filterFormMap.put("qsfood=1","QS食品安全");
		filterFormMap.put("user_type=1","正品保障");
		filterFormMap.put("promote=2097152","海外商品");
		filterFormMap.put("game_js=1","寄售交易");
		filterFormMap.put("offpayment=1","服务信息");
		filterFormMap.put("sr=1","尺码推荐");
		filterFormMap.put("dtsp=1","细节实拍");
		filterFormMap.put("onsale_actcard=1:101;2:120","淘宝代购");
		filterFormMap.put("isprepay=1","消费者保障");
		filterFormMap.put("onsale_vipcard=1:101;2:119;","VIP");
		filterFormMap.put("wtr=1","网厅推荐");
		filterFormMap.put("olu=yes","旺旺在线");
		filterFormMap.put("third_qc=1","第三方质检");
		filterFormMap.put("game_db=1","担保交易");
		filterFormMap.put("offpayment=1","无需在线支付");
		filterFormMap.put("appointment=1","预约信息");
		filterFormMap.put("ec=1","电子兑换券");
		filterFormMap.put("closeup=1","细节特写");
		filterFormMap.put("exp=1","123时效");
		filterFormMap.put("photo=1","淘摄影");
		filterFormMap.put("viewIndex=4","二手");
		filterFormMap.put("viewIndex=6","拍卖");
		filterFormMap.put("viewIndex=10","商城");
		filterFormMap.put("viewIndex=11","新品");
		filterFormMap.put("viewIndex=1","全新");
	}
    public static void main(String[] args) throws IOException {
        String input = args[0];
        if (!new File(input).exists()) {
            System.out.println("File Not Exist ! => " + input);
            return;
        }
        Report report = Report.newReport("list2.0筛选项的使用情况统计");
        Table filterFormTable = report.newTable("filterForm", "筛选项的使用情况统计");

        for (String line : Utils.readWithCharset(input, "utf-8")) {
            String[] _allCols = StringUtils.splitPreserveAllTokens(line, CTRL_A);
            if (_allCols.length > 2) {
            	String key = _allCols[0]+_allCols[1];
            	if(StringUtils.isNotEmpty(filterFormMap.get(key))){
            		filterFormTable.addCol(key,_allCols[2]);
            	}
            }
        }
        filterFormTable.sort(Table.SORT_VALUE);
        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
    }
}