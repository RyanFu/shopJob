//===================================================================================
// Copyright (c) 2004-2012 by www.taobao.com, All rights reserved.
// 9F., ChuangYe building, 99# huaxing road, HangZhou, China
// 
// This software is the confidential and proprietary information of 
// Taobao.com, Inc. ("Confidential Information"). You shall not disclose 
// such Confidential Information and shall use it only in accordance 
// with the terms of the license agreement you entered into with Taobao.com, Inc.
//===================================================================================
// File name: NewCarOfflinePayMoney.java
// Author: longque.zs
// Date: 2012-9-18 下午07:35:37 
// Description: 	 
// 		无
// Function List: 	 
// 		1. 无
// History: 
// 		1. 无
//===================================================================================

package org.dueam.hadoop.bp.report.offlinepay;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

/**
 * 线下支付-汽车新车类目pos刷卡金额(包括汽车新车整车类目(50050565)、汽车新车全款类目(50024973)、汽车新车定金(50018718))
 * @author longque.zs
 * @version 1.0
 **/

public class NewCarOfflinePayMoney {
	
	private static char CTRL_A=(char)0x01; //分隔符

	/**
	 * 主要内容：
	 * 汽车新车类目线上小定成交排行榜(每天)：卖家用户名和宝贝id分组  筛选内容：卖家用户名、商品id、成交笔数、支付宝成交金额
	 * 汽车新车类目线下POS大定成交排行榜(每天)：卖家用户名和宝贝id分组  筛选内容：卖家用户名、商品id、成交笔数、支付宝成交金额
	 * 汽车新车类目2012年线上小定成交排行榜(每年)：卖家用户名分组  筛选内容：卖家用户名、成交笔数、支付宝成交金额
	 * 汽车新车类目2012年线下POS大定成交排行榜(每年)：卖家用户名分组  筛选内容：卖家用户名、成交笔数、支付宝成交金额
	 * @author longque.zs
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args)throws IOException{
//		String input = "D:\\usr\\20121029"; // 文件路径
		String mainName="线下支付-汽车新车"; //报表主名称
		String input=args[0]; //文件路径
		Report report = Report.newReport(mainName+"交易数据");
		String smallOrder_Perday="smallOrderPerday"; //每天小定
		String bigOrder_Perday="bigOrderPerday"; //每天大定
		String smallOrder2012="smallOrder2012"; //2012小定
		String bigOrder2012="bigOrder2012"; //2012大定
		String smallOrder2013="smallOrder2013"; //2013小定
		String bigOrder2013="bigOrder2013"; //2013大定
		
		//pos小定-每天
		String preUserNickSmall=""; //小定每个卖家昵称
		int preUserSmallNum=0; //小定每个卖家成交数目
		double preUserSamllGMV = 0; //小定每个卖家成交金额
		int smallCount=0;	//小定成交数目
		double smallFee=0; //小定成交金额
		
		//pos大定-每天
		String preUserNickBig=""; //大定每个卖家昵称
		int preUserBigNum=0; //大定每个卖家成交数目
		double preUserBigGMV=0; //大定每个卖家成交金额
		int bigCount=0;	//大定成交数目
		double bigFee=0; //大定成交金额
		
		//pos小定-2012每年(成交笔数和成交金额)
		int smallOrder2012_count=0;
		double smallOrder2012_fee=0;
		
		//pos大定-2012每年(成交笔数和成交金额)
		int bigOrder2012_count=0;
		double bigOrder2012_fee=0;
		
		//pos小定-2013每年(成交笔数和成交金额)
		int smallOrder2013_count=0;
		double smallOrder2013_fee=0;
		
		//pos大定-2013每年(成交笔数和成交金额)
		int bigOrder2013_count=0;
		double bigOrder2013_fee=0;
		if (!new File(input).exists()) {
			System.out.println("File Not Exist !" + input);
			return;
		}
		List<String> lines = Utils.readWithCharset(input, "utf-8");
		
		//汽车新车线上小定成交排行榜(每天)
		Table smallOrderPerdayTable = report.newViewTable(smallOrder_Perday, mainName+"线上小定成交排行榜");
		smallOrderPerdayTable.addCol("卖家昵称").addCol("商品id").addCol("成交笔数").addCol("支付宝成交金额").addCol(Report.BREAK_VALUE);;
		//汽车新车线下POS大定成交排行榜(每天)
		Table bigOrderPerdayTable = report.newViewTable(bigOrder_Perday, mainName+"线下POS大定成交排行榜");
		bigOrderPerdayTable.addCol("卖家昵称").addCol("商品id").addCol("成交笔数").addCol("支付宝成交金额").addCol(Report.BREAK_VALUE);
		//汽车新车2012年线上小定成交排行榜(每年)
		Table smallOrder2012Table = report.newViewTable(smallOrder2012, mainName+"2012年线上小定成交排行榜");
		smallOrder2012Table.addCol("卖家昵称").addCol("成交笔数").addCol("支付宝成交金额").addCol(Report.BREAK_VALUE);
		//汽车新车2012年线下POS大定成交排行榜(每年)
		Table bigOrder2012Table = report.newViewTable(bigOrder2012, mainName+"2012年线下POS大定成交排行榜");
		bigOrder2012Table.addCol("卖家昵称").addCol("成交笔数").addCol("支付宝成交金额").addCol(Report.BREAK_VALUE);
		//汽车新车2013年线上小定成交排行榜(每年)
		Table smallOrder2013Table = report.newViewTable(smallOrder2013, mainName+"2013年线上小定成交排行榜");
		smallOrder2013Table.addCol("卖家昵称").addCol("成交笔数").addCol("支付宝成交金额").addCol(Report.BREAK_VALUE);
		//汽车新车2013年线下POS大定成交排行榜(每年)
		Table bigOrder2013Table = report.newViewTable(bigOrder2013, mainName+"2013年线下POS大定成交排行榜");
		bigOrder2013Table.addCol("卖家昵称").addCol("成交笔数").addCol("支付宝成交金额").addCol(Report.BREAK_VALUE);
		
		if (lines!= null && lines.size() > 0) {
			for(String line:lines){
				String[] _cols=StringUtils.split(line,CTRL_A);
				if((_cols!=null&&_cols.length>2)&&(_cols[1].contains("测试")||_cols[1].contains("龙阙"))){
					continue;
				}
				if(line.startsWith(smallOrder_Perday)){ //pos小定-每天(成交笔数和成交金额)
					if(_cols.length > 0){
						if(StringUtils.isEmpty(preUserNickSmall)||StringUtils.equals(preUserNickSmall,_cols[1])){
							preUserSmallNum=preUserSmallNum+Integer.valueOf(_cols[3]);
							preUserSamllGMV=preUserSamllGMV+Double.parseDouble(_cols[4]);
						}else{
							smallOrderPerdayTable.addCol(preUserNickSmall+"小定合计");
							smallOrderPerdayTable.addCol(preUserNickSmall+"大定合计ItemId","");
							smallOrderPerdayTable.addCol(preUserNickSmall+"大定合计数目",preUserSmallNum+"");
							smallOrderPerdayTable.addCol(preUserNickSmall+"大定合计GMV",preUserSamllGMV+"");
							smallOrderPerdayTable.breakRow();
							preUserSmallNum=0;
							preUserSamllGMV=0;
							preUserSmallNum=preUserSmallNum+Integer.valueOf(_cols[3]);
							preUserSamllGMV=preUserSamllGMV+Double.parseDouble(_cols[4]);
						}
						
						smallOrderPerdayTable.addCol(_cols[1]);
						smallOrderPerdayTable.addCol(_cols[1] + "ITEMID", _cols[2]);
						smallOrderPerdayTable.addCol(_cols[1] + "NUM", _cols[3]);
						smallOrderPerdayTable.addCol(_cols[1] + "GMV", _cols[4]);
						smallOrderPerdayTable.breakRow();
						preUserNickSmall=_cols[1];
						smallCount=smallCount+Integer.valueOf(_cols[3]);
						smallFee=smallFee+Double.parseDouble(_cols[4]);
					}
				}else if(line.startsWith(bigOrder_Perday)){ //pos大定-每天(成交笔数和成交金额)
					if(_cols.length > 0){
						if(StringUtils.isEmpty(preUserNickBig)||StringUtils.equals(preUserNickBig,_cols[1])){
							preUserBigNum=preUserBigNum+Integer.valueOf(_cols[3]);
							preUserBigGMV=preUserBigGMV+Double.parseDouble(_cols[4]);
						}else{
							bigOrderPerdayTable.addCol(preUserNickBig+"大定合计");
							bigOrderPerdayTable.addCol(preUserNickBig+"大定合计ItemId","");
							bigOrderPerdayTable.addCol(preUserNickBig+"大定合计数目",preUserBigNum+"");
							bigOrderPerdayTable.addCol(preUserNickBig+"大定合计GMV",preUserBigGMV+"");
							bigOrderPerdayTable.breakRow();
							preUserBigNum=0;
							preUserBigGMV=0;
							preUserBigNum=preUserBigNum+Integer.valueOf(_cols[3]);
							preUserBigGMV=preUserBigGMV+Double.parseDouble(_cols[4]);
						}
						
						bigOrderPerdayTable.addCol(_cols[1]);
						bigOrderPerdayTable.addCol(_cols[1] + "ITEMID", _cols[2]);
						bigOrderPerdayTable.addCol(_cols[1] + "NUM", _cols[3]);
						bigOrderPerdayTable.addCol(_cols[1] + "GMV", _cols[4]);
						bigOrderPerdayTable.breakRow();
						preUserNickBig=_cols[1];
						bigCount=bigCount+Integer.valueOf(_cols[3]);
						bigFee=bigFee+Double.parseDouble(_cols[4]);
					}
					
				}else if(line.startsWith(smallOrder2012)){ //pos小定-每年
					smallOrder2012Table.addCol(_cols[1]);
					smallOrder2012Table.addCol(_cols[1] + "NUM", _cols[2]);
					smallOrder2012Table.addCol(_cols[1] + "GMV", _cols[3]);
					smallOrder2012Table.breakRow();
					smallOrder2012_count=smallOrder2012_count+Integer.valueOf(_cols[2]);
					smallOrder2012_fee=smallOrder2012_fee+Double.parseDouble(_cols[3]);
				}else if(line.startsWith(bigOrder2012)){ ////pos大定-每年
					bigOrder2012Table.addCol(_cols[1]);
					bigOrder2012Table.addCol(_cols[1] + "NUM", _cols[2]);
					bigOrder2012Table.addCol(_cols[1] + "GMV", _cols[3]);
					bigOrder2012Table.breakRow();
					bigOrder2012_count=bigOrder2012_count+Integer.valueOf(_cols[2]);
					bigOrder2012_fee=bigOrder2012_fee+Double.parseDouble(_cols[3]);
				}else if(line.startsWith(smallOrder2013)){ //pos小定-每年
					smallOrder2013Table.addCol(_cols[1]);
					smallOrder2013Table.addCol(_cols[1] + "NUM", _cols[2]);
					smallOrder2013Table.addCol(_cols[1] + "GMV", _cols[3]);
					smallOrder2013Table.breakRow();
					smallOrder2013_count=smallOrder2013_count+Integer.valueOf(_cols[2]);
					smallOrder2013_fee=smallOrder2013_fee+Double.parseDouble(_cols[3]);
				}else if(line.startsWith(bigOrder2013)){ //pos大定-每年
					bigOrder2013Table.addCol(_cols[1]);
					bigOrder2013Table.addCol(_cols[1] + "NUM", _cols[2]);
					bigOrder2013Table.addCol(_cols[1] + "GMV", _cols[3]);
					bigOrder2013Table.breakRow();
					bigOrder2013_count=bigOrder2013_count+Integer.valueOf(_cols[2]);
					bigOrder2013_fee=bigOrder2013_fee+Double.parseDouble(_cols[3]);
				}
			}
			
			//pos小定-每天
			smallOrderPerdayTable.addCol(preUserNickSmall+"小定合计");
			smallOrderPerdayTable.addCol(preUserNickSmall+"小定合计itemid","");
			smallOrderPerdayTable.addCol(preUserNickSmall+"小定合计数量",preUserSmallNum+"");
			smallOrderPerdayTable.addCol(preUserNickSmall+"小定合计GMV",preUserSamllGMV+"");
			smallOrderPerdayTable.breakRow();
			
			smallOrderPerdayTable.addCol("小定成交合计");
			smallOrderPerdayTable.addCol("");
			smallOrderPerdayTable.addCol("小定成交合计NUM",String.valueOf(smallCount));
			smallOrderPerdayTable.addCol("小定成交合计GMV",String.valueOf(smallFee));
			smallOrderPerdayTable.breakRow();
			
			//pos大定-每天
			bigOrderPerdayTable.addCol(preUserNickBig+"大定合计");
			bigOrderPerdayTable.addCol(preUserNickBig+"大定合计itemid","");
			bigOrderPerdayTable.addCol(preUserNickBig+"大定合计数量",preUserBigNum+"");
			bigOrderPerdayTable.addCol(preUserNickBig+"大定合计GMV",preUserBigGMV+"");
			bigOrderPerdayTable.breakRow();
			
			bigOrderPerdayTable.addCol("大定成交合计");
			bigOrderPerdayTable.addCol("");
			bigOrderPerdayTable.addCol("大定成交合计NUM",String.valueOf(bigCount));
			bigOrderPerdayTable.addCol("大定成交合计GMV",String.valueOf(bigFee));
			bigOrderPerdayTable.breakRow();
			
			//pos小定-2012每年
			smallOrder2012Table.addCol("2012小定成交合计");
			smallOrder2012Table.addCol("2012小定成交合计NUM",String.valueOf(smallOrder2012_count));
			smallOrder2012Table.addCol("2012小定成交合计GMV",String.valueOf(smallOrder2012_fee));
			smallOrder2012Table.breakRow();
			
			//pos大定-2012每年
			bigOrder2012Table.addCol("2012大定成交合计");
			bigOrder2012Table.addCol("2012大定成交合计NUM",String.valueOf(bigOrder2012_count));
			bigOrder2012Table.addCol("2012大定成交合计GMV",String.valueOf(bigOrder2012_fee));
			bigOrder2012Table.breakRow();
			
			//pos小定-2013每年
			smallOrder2013Table.addCol("2013小定成交合计");
			smallOrder2013Table.addCol("2013小定成交合计NUM",String.valueOf(smallOrder2013_count));
			smallOrder2013Table.addCol("2013小定成交合计GMV",String.valueOf(smallOrder2013_fee));
			smallOrder2013Table.breakRow();
			
			//pos大定-2013每年
			bigOrder2013Table.addCol("2013大定成交合计");
			bigOrder2013Table.addCol("2013大定成交合计NUM",String.valueOf(bigOrder2013_count));
			bigOrder2013Table.addCol("2013大定成交合计GMV",String.valueOf(bigOrder2013_fee));
			bigOrder2013Table.breakRow();
		} 
		
		XmlReportFactory.dump(report, new FileOutputStream(input + ".xml"));
	}
}
