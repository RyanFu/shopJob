package org.dueam.hadoop.bp.report;

import org.apache.commons.lang.StringUtils;
import org.dueam.hadoop.common.util.Fmt;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * 跳蚤街list统计报表
 */
public class ErshouReport {
	 private static char CTRL_A = (char) 0x01;
	    public static void main(String[] args) throws IOException {
	        String input = args[0];
	        //String input = "d:/file.txt" ;
	        if (!new File(input).exists()) {
	            System.out.println("File Not Exist ! => " + input);
	            return;
	        }
	        Report report = Report.newReport("跳蚤街list统计报表");
	        Table detailTable = report.newViewTable("detailTable", "浏览数据汇总");
	        Table tradeTable = report.newViewTable("tradeTable", "交易数据汇总");
	        Table totalTable = report.newTable("total", "总体数据汇总");

	        int totalPv = 0, totalUv = 0,ipv = 0,
	            ipvUv = 0, ali_totalMoney = 0, ali_buyerUv = 0,
	            ali_orderCount = 0,ali_itemUv = 0, ali_sellerUv = 0,
	            gmv_totalMoney = 0, gmv_buyerUv = 0,
	            gmv_orderCount = 0,gmv_itemUv = 0, gmv_sellerUv = 0;

	        for (String line : Utils.readWithCharset(input, "utf-8")) {
	            String[] _allCols = StringUtils.splitPreserveAllTokens(line, CTRL_A);

	            if (_allCols.length > 2) {
	                if (StringUtils.equals(_allCols[0], "ls")) {
	                    totalPv = Integer.parseInt(_allCols[1]);
	                    totalUv = Integer.parseInt(_allCols[2]);
	                }else if (StringUtils.equals(_allCols[0], "ipv")) {
	                    ipv = Integer.parseInt(_allCols[1]);
	                    ipvUv = Integer.parseInt(_allCols[2]);
	                }else if (StringUtils.equals(_allCols[0], "trade_ali")) {
	                    ali_totalMoney = Integer.parseInt(_allCols[1]);
	                    ali_itemUv = Integer.parseInt(_allCols[2]);
	                    ali_buyerUv = Integer.parseInt(_allCols[3]);
	                    ali_sellerUv = Integer.parseInt(_allCols[4]);
	                    ali_orderCount = Integer.parseInt(_allCols[5]);
	                }else if (StringUtils.equals(_allCols[0], "trade_gmv")) {
	                    gmv_totalMoney = Integer.parseInt(_allCols[1]);
	                    gmv_itemUv = Integer.parseInt(_allCols[2]);
	                    gmv_buyerUv = Integer.parseInt(_allCols[3]);
	                    gmv_sellerUv = Integer.parseInt(_allCols[4]);
	                    gmv_orderCount = Integer.parseInt(_allCols[5]);
	                }

	            }
	        }
	        String pvRate = Fmt.div(totalPv, totalUv);  // pv/人
	        String detailRate = Fmt.parent2(String.valueOf(ipvUv), String.valueOf(totalUv));//浏览转化率
	        String moneyPerson = Fmt.div(ali_totalMoney, ali_buyerUv);  //客单价
	        String buyRate = Fmt.parent2(String.valueOf(ali_buyerUv), String.valueOf(ipvUv));

	        detailTable.addCol("PV").
	                addCol("UV").
	                addCol("IPV").
	                addCol("IPV-UV").
	                addCol("浏览转化率").
	                addCol("pv/人").
	                addCol(Report.BREAK_VALUE);
	        detailTable.addCol(String.valueOf(totalPv)).
	                addCol(String.valueOf(totalUv)).
	                addCol(String.valueOf(ipv)).
	                addCol(String.valueOf(ipvUv)).
	                addCol(String.valueOf(detailRate)).
	                addCol(String.valueOf(pvRate)).
	                addCol(Report.BREAK_VALUE);

	        tradeTable.addCol("支付宝金额").
	                addCol("IPV-UV").
	                addCol("购买UV").
	                addCol("购买转化率").
	                addCol("订单数").
	                addCol("客单价").
	                addCol(Report.BREAK_VALUE);

	        tradeTable.addCol(String.valueOf(ali_totalMoney)).
	                addCol(String.valueOf(ipvUv)).
	                addCol(String.valueOf(ali_buyerUv)).
	                addCol(String.valueOf(buyRate)).     //购买抓化率
	                addCol(String.valueOf(ali_orderCount)).
	                addCol(String.valueOf(moneyPerson)).
	                addCol(Report.BREAK_VALUE);

	        totalTable.addCol("totalPv", "PV", String.valueOf(totalPv));
	        totalTable.addCol("totaluv", "UV", String.valueOf(totalUv));
	        totalTable.addCol("pvRate", "PV/人", String.valueOf(totalPv/totalUv));

	        totalTable.addCol("ipv","IPV" ,String.valueOf(ipv)) ;
	        totalTable.addCol("ipvUv","IPV-UV" ,String.valueOf(ipvUv)) ;

	        totalTable.addCol("gmv_uv", "gmv成交UV", String.valueOf(gmv_buyerUv));
	        totalTable.addCol("gmv_orderCount", "gmv成交笔数", String.valueOf(gmv_orderCount));
	        totalTable.addCol("gmv_totalMoney", "gmv成交金额", String.valueOf(gmv_totalMoney));
	        totalTable.addCol("gmv_itemNumber", "gmv成交件数", String.valueOf(gmv_itemUv));

	        totalTable.addCol("alipay_uv", "支付宝成交UV", String.valueOf(ali_buyerUv));
	        totalTable.addCol("alipay_orderCount", "支付宝成交笔数", String.valueOf(ali_orderCount));
	        totalTable.addCol("alipay_totalMoney", "支付宝成交金额", String.valueOf(ali_totalMoney));
	        totalTable.addCol("alipay_itemNumber", "支付宝成交件数", String.valueOf(ali_itemUv));

	        //totalTable.addCol("totalmoney", "总成交额", String.valueOf(ali_totalMoney));
	       // totalTable.addCol("moneyPerson", "客单价", String.valueOf(ali_totalMoney/ali_buyerUv));
	        totalTable.addCol("sellerUv", "有交易的卖家", String.valueOf(ali_sellerUv));
	        totalTable.addCol("detailRate","浏览转化率*10000" ,String.valueOf(ipvUv*10000/totalUv)) ;
	        totalTable.addCol("buyRate","购买转化率*10000" ,String.valueOf(ali_buyerUv*10000/ipvUv)) ;

	        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
	    }
}
