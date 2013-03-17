/**
 * 
 */
package org.dueam.hadoop.bp.report.offlinepay;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

/**
 * @author longjia.zt
 *
 */
public class OfflineTrade {
	private static char CTRL_A=(char)0x01; //分隔符
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		//String input = "D:\\20121007"; // 文件路径
		String mainName="线下支付"; //报表主名称
		String input=args[0]; //文件路径
		//卖家维度
		String bigOneday_seller="bigOneday_seller"; // 
		String bigOneyear_seller="bigOneyear_seller"; //
		String smallOneday_seller="smallOneday_seller"; // 
		String smallOneyear_seller="smallOneyear_seller"; // 
		//类目商品维度
		String bigoneday_leimu_item="bigoneday_leimu_item"; // 
		String smalloneday_leimu_item="smalloneday_leimu_item"; // 
		//类目成交金额维度
		String bigOneYear_leimu_amount="bigOneYear_leimu_amount"; // 
		String bigOneDay_leimu_amount="bigOneDay_leimu_amount"; //
		String smallOneYear_leimu_amount="smallOneYear_leimu_amount"; // 
		String smallOneDay_leimu_amount="smallOneDay_leimu_amount"; // 

		Report report = Report.newReport(mainName + "交易数据统计");

		//类目成交金额维度-------------------------------
		Table smallOneDay_leimu_amountTable = report.newViewTable(smallOneDay_leimu_amount, mainName+"今天小定定类目成交");
		smallOneDay_leimu_amountTable.addCol("主类目").addCol("叶子类目").addCol("成交笔数").addCol("成交金额").addCol(Report.BREAK_VALUE);;
		
		
		Table bigOneDay_leimu_amountTable = report.newViewTable(bigOneDay_leimu_amount, mainName+"今天大定类目成交");
		bigOneDay_leimu_amountTable.addCol("主类目").addCol("叶子类目").addCol("成交笔数").addCol("成交金额").addCol(Report.BREAK_VALUE);;
		
		
		Table smallOneYear_leimu_amountTable = report.newViewTable(smallOneYear_leimu_amount, mainName+"今年小定类目成交");
		smallOneYear_leimu_amountTable.addCol("主类目").addCol("叶子类目").addCol("成交笔数").addCol("成交金额").addCol(Report.BREAK_VALUE);;
		
		Table bigOneYear_leimu_amountTable = report.newViewTable(bigOneYear_leimu_amount, mainName+"今年大定类目成交");
		bigOneYear_leimu_amountTable.addCol("主类目").addCol("叶子类目").addCol("成交笔数").addCol("成交金额").addCol(Report.BREAK_VALUE);;
		
		
		//类目成交金额维度-------------------------------结束
		
		//类目商品维度--------------------------
		Table smalloneday_leimu_itemTable = report.newViewTable(smalloneday_leimu_item, mainName+"今天小定热门宝贝成交");
		smalloneday_leimu_itemTable.addCol("主类目").addCol("叶子类目").addCol("商品ID").addCol("成交笔数").addCol("成交金额").addCol(Report.BREAK_VALUE);
		
		Table bigoneday_leimu_itemTable = report.newViewTable(bigoneday_leimu_item, mainName+"今天大定热门宝贝成交");
		bigoneday_leimu_itemTable.addCol("主类目").addCol("叶子类目").addCol("商品ID").addCol("成交笔数").addCol("成交金额").addCol(Report.BREAK_VALUE);
		
		//类目商品维度--------------------------结束
		
		
		//卖家维度------------------------------------
		Table smallOneday_sellerTable = report.newViewTable(smallOneday_seller, mainName+"卖家今天小定成交");
		smallOneday_sellerTable.addCol("卖家昵称").addCol("成交笔数").addCol("成交金额").addCol(Report.BREAK_VALUE);;
		
		Table bigOneday_sellerTable = report.newViewTable(bigOneday_seller, mainName+"卖家今天大定成交");
		bigOneday_sellerTable.addCol("卖家昵称").addCol("成交笔数").addCol("成交金额").addCol(Report.BREAK_VALUE);;
		
		Table smallOneyear_sellerTable = report.newViewTable(smallOneyear_seller, mainName+"卖家今年小定定成交");
		smallOneyear_sellerTable.addCol("卖家昵称").addCol("成交笔数").addCol("成交金额").addCol(Report.BREAK_VALUE);;
		
		Table bigOneyear_sellerTable = report.newViewTable(bigOneyear_seller, mainName+"卖家今年大定成交");
		bigOneyear_sellerTable.addCol("卖家昵称").addCol("成交笔数").addCol("成交金额").addCol(Report.BREAK_VALUE);;
	
		//卖家维度----------------------------结束


		if (!new File(input).exists()) {
			System.out.println("File Not Exist !" + input);
			return;
		}
		List<String> lines = Utils.readWithCharset(input, "utf-8");
		
		String smallOneDay_leimu_amountPre="";
		int smallOneDay_leimu_amountCount=0;
		Double smallOneDay_leimu_amountAmount=0.0;
		
		int smallOneDay_leimu_amountTotalCount=0;
		Double smallOneDay_leimu_amountTotalAmount=0.0;
		
		
		
		
		String bigOneDay_leimu_amountPre="";
		int bigOneDay_leimu_amountCount=0;
		Double bigOneDay_leimu_amountAmount=0.0;
		int bigOneDay_leimu_amountTotalCount=0;
		Double bigOneDay_leimu_amountTotalAmount=0.0;
		
		
		
		String smallOneYear_leimu_amountPre="";
		int smallOneYear_leimu_amountCount=0;
		Double smallOneYear_leimu_amountAmount=0.0;
		int smallOneYear_leimu_amountTotalCount=0;
		Double smallOneYear_leimu_amountTotalAmount=0.0;
		
		
		String bigOneYear_leimu_amountPre="";
		int bigOneYear_leimu_amountCount=0;
		Double bigOneYear_leimu_amountAmount=0.0;
		int bigOneYear_leimu_amountTotalCount=0;
		Double bigOneYear_leimu_amountTotalAmount=0.0;
		
		
		
		
		
		String smalloneday_leimu_itemPre="";
		int smalloneday_leimu_itemCount=0;
		Double smalloneday_leimu_itemAmount=0.0;
		int smalloneday_leimu_itemTotalCount=0;
		Double smalloneday_leimu_itemTotalAmount=0.0;
		
		
		String bigoneday_leimu_itemPre="";
		int bigoneday_leimu_itemCount=0;
		Double bigoneday_leimu_itemAmount=0.0;
		int bigoneday_leimu_itemTotalCount=0;
		Double bigoneday_leimu_itemTotalAmount=0.0;
		
		
		
		if (lines!= null && lines.size() > 0) {
			for(String line:lines){
				String[] _cols=StringUtils.split(line,CTRL_A);
				 
				if(smallOneDay_leimu_amount.equals(_cols[0])){
					 if(""==smallOneDay_leimu_amountPre){
						 smallOneDay_leimu_amountPre= _cols[1];
						 smallOneDay_leimu_amountCount=Integer.parseInt(_cols[3]);
						 smallOneDay_leimu_amountAmount=Double.parseDouble(_cols[4]);
						 smallOneDay_leimu_amountTable.addCol(_cols[1]);
					 }else if(_cols[1].equals(smallOneDay_leimu_amountPre)){//相等 说明是同一个类目
						 smallOneDay_leimu_amountCount+= Integer.parseInt(_cols[3]);
						 smallOneDay_leimu_amountAmount+=Double.parseDouble(_cols[4]);
						 smallOneDay_leimu_amountTable.addCol("--");
					 }
					 else{//不相等
						smallOneDay_leimu_amountTable.addCol(smallOneDay_leimu_amountPre+"合计：");
						smallOneDay_leimu_amountTable.addCol("--");
						smallOneDay_leimu_amountTable.addCol((int)smallOneDay_leimu_amountCount+"");
						smallOneDay_leimu_amountTable.addCol(smallOneDay_leimu_amountAmount.longValue()+"");
						smallOneDay_leimu_amountTable.breakRow();
						
						smallOneDay_leimu_amountTotalCount+=smallOneDay_leimu_amountCount;
						smallOneDay_leimu_amountTotalAmount+=smallOneDay_leimu_amountAmount;
						
						smallOneDay_leimu_amountPre= _cols[1];
						
						 smallOneDay_leimu_amountCount=Integer.parseInt(_cols[3]);
						 smallOneDay_leimu_amountAmount=Double.parseDouble(_cols[4]);
						 
						 smallOneDay_leimu_amountTable.addCol(_cols[1]);
					 }
					
					smallOneDay_leimu_amountTable.addCol(_cols[2]);
					smallOneDay_leimu_amountTable.addCol(_cols[3]);
					smallOneDay_leimu_amountTable.addCol(_cols[4]);
					smallOneDay_leimu_amountTable.breakRow();
					
				}else if(bigOneDay_leimu_amount.equals(_cols[0])){
					
					if(""==bigOneDay_leimu_amountPre){
						bigOneDay_leimu_amountPre= _cols[1];
						 bigOneDay_leimu_amountCount=Integer.parseInt(_cols[3]);
						 bigOneDay_leimu_amountAmount=Double.parseDouble(_cols[4]);
						 bigOneDay_leimu_amountTable.addCol(_cols[1]);
					 }else if(_cols[1].equals(bigOneDay_leimu_amountPre)){//相等 说明是同一个类目
						 bigOneDay_leimu_amountCount+= Integer.parseInt(_cols[3]);
						 bigOneDay_leimu_amountAmount+=Double.parseDouble(_cols[4]);
						 bigOneDay_leimu_amountTable.addCol("--");
					 }
					 else{//不相等
						bigOneDay_leimu_amountTable.addCol(bigOneDay_leimu_amountPre+"合计：");
						bigOneDay_leimu_amountTable.addCol("--");
						bigOneDay_leimu_amountTable.addCol(bigOneDay_leimu_amountCount+"");
						bigOneDay_leimu_amountTable.addCol(bigOneDay_leimu_amountAmount.longValue()+"");
						bigOneDay_leimu_amountTable.breakRow();
						
						bigOneDay_leimu_amountTotalCount+=bigOneDay_leimu_amountCount;
						bigOneDay_leimu_amountTotalAmount+=bigOneDay_leimu_amountAmount;
						
						bigOneDay_leimu_amountPre= _cols[1];
						
						 bigOneDay_leimu_amountCount=Integer.parseInt(_cols[3]);
						 bigOneDay_leimu_amountAmount=Double.parseDouble(_cols[4]);
						 
						 bigOneDay_leimu_amountTable.addCol(_cols[1]);
					 }
					
					
					bigOneDay_leimu_amountTable.addCol(_cols[2]);
					bigOneDay_leimu_amountTable.addCol(_cols[3]);
					bigOneDay_leimu_amountTable.addCol(_cols[4]);
					bigOneDay_leimu_amountTable.breakRow();
				}else if(smallOneYear_leimu_amount.equals(_cols[0])){
					
					 if(""==smallOneYear_leimu_amountPre){
						 smallOneYear_leimu_amountPre= _cols[1];
						 smallOneYear_leimu_amountCount=Integer.parseInt(_cols[3]);
						 smallOneYear_leimu_amountAmount=Double.parseDouble(_cols[4]);
						 smallOneYear_leimu_amountTable.addCol(_cols[1]);
					 }else if(_cols[1].equals(smallOneYear_leimu_amountPre)){//相等 说明是同一个类目
						 smallOneYear_leimu_amountCount+= Integer.parseInt(_cols[3]);
						 smallOneYear_leimu_amountAmount+=Double.parseDouble(_cols[4]);
						 smallOneYear_leimu_amountTable.addCol("--");
					 }
					 else{//不相等
						smallOneYear_leimu_amountTable.addCol(smallOneYear_leimu_amountPre+"合计：");
						smallOneYear_leimu_amountTable.addCol("--");
						smallOneYear_leimu_amountTable.addCol(smallOneYear_leimu_amountCount+"");
						smallOneYear_leimu_amountTable.addCol(smallOneYear_leimu_amountAmount.longValue()+"");
						smallOneYear_leimu_amountTable.breakRow();
						
						smallOneYear_leimu_amountTotalCount+=smallOneYear_leimu_amountCount;
						smallOneYear_leimu_amountTotalAmount+=smallOneYear_leimu_amountAmount;
						
						smallOneYear_leimu_amountPre= _cols[1];
						
						 smallOneYear_leimu_amountCount=Integer.parseInt(_cols[3]);
						 smallOneYear_leimu_amountAmount=Double.parseDouble(_cols[4]);
						 
						 smallOneYear_leimu_amountTable.addCol(_cols[1]);
					 }					
					smallOneYear_leimu_amountTable.addCol(_cols[2]);
					smallOneYear_leimu_amountTable.addCol(_cols[3]);
					smallOneYear_leimu_amountTable.addCol(_cols[4]);
					smallOneYear_leimu_amountTable.breakRow();
				}else if(bigOneYear_leimu_amount.equals(_cols[0])){
					
					
					if(""==bigOneYear_leimu_amountPre){
						 bigOneYear_leimu_amountPre= _cols[1];
						 bigOneYear_leimu_amountCount=Integer.parseInt(_cols[3]);
						 bigOneYear_leimu_amountAmount=Double.parseDouble(_cols[4]);
						 bigOneYear_leimu_amountTable.addCol(_cols[1]);
					 }else if(_cols[1].equals(bigOneYear_leimu_amountPre)){//相等 说明是同一个类目
						 bigOneYear_leimu_amountCount+= Integer.parseInt(_cols[3]);
						 bigOneYear_leimu_amountAmount+=Double.parseDouble(_cols[4]);
						 bigOneYear_leimu_amountTable.addCol("--");
					 }
					 else{//不相等
						bigOneYear_leimu_amountTable.addCol(bigOneYear_leimu_amountPre+"合计：");
						bigOneYear_leimu_amountTable.addCol("--");
						bigOneYear_leimu_amountTable.addCol((int)bigOneYear_leimu_amountCount+"");
						bigOneYear_leimu_amountTable.addCol(bigOneYear_leimu_amountAmount.longValue()+"");
						bigOneYear_leimu_amountTable.breakRow();
						
						bigOneYear_leimu_amountTotalCount+=bigOneYear_leimu_amountCount;
						bigOneYear_leimu_amountTotalAmount+=bigOneYear_leimu_amountAmount;
						
						bigOneYear_leimu_amountPre= _cols[1];
						
						 bigOneYear_leimu_amountCount=Integer.parseInt(_cols[3]);
						 bigOneYear_leimu_amountAmount=Double.parseDouble(_cols[4]);
						 
						 bigOneYear_leimu_amountTable.addCol(_cols[1]);
					 }

					bigOneYear_leimu_amountTable.addCol(_cols[2]);
					bigOneYear_leimu_amountTable.addCol(_cols[3]);
					bigOneYear_leimu_amountTable.addCol(new BigDecimal(_cols[4]).toPlainString());
					bigOneYear_leimu_amountTable.breakRow();
				}
				//类目成交金额维度-------------------------------结束
				else if(bigoneday_leimu_item.equals(_cols[0])){
		
					if(""==bigoneday_leimu_itemPre){
						bigoneday_leimu_itemPre= _cols[1];
						bigoneday_leimu_itemCount=Integer.parseInt(_cols[4]);
						bigoneday_leimu_itemAmount=Double.parseDouble(_cols[5]);
						bigoneday_leimu_itemTable.addCol(_cols[1]);
					 }else if(_cols[1].equals(bigoneday_leimu_itemPre)){//相等 说明是同一个类目
						 bigoneday_leimu_itemCount+= Integer.parseInt(_cols[4]);
						 bigoneday_leimu_itemAmount+=Double.parseDouble(_cols[5]);
						 bigoneday_leimu_itemTable.addCol("--");
					 }
					 else{//不相等
						 bigoneday_leimu_itemTable.addCol(bigoneday_leimu_itemPre+"合计：");
						 bigoneday_leimu_itemTable.addCol("--");
						 bigoneday_leimu_itemTable.addCol("--");
						 bigoneday_leimu_itemTable.addCol(bigoneday_leimu_itemCount+"");
						 bigoneday_leimu_itemTable.addCol(bigoneday_leimu_itemAmount+"");
						 bigoneday_leimu_itemTable.breakRow();
						
						 bigoneday_leimu_itemPre= _cols[1];
						
						 bigoneday_leimu_itemTotalCount+=bigoneday_leimu_itemCount;
						 bigoneday_leimu_itemTotalAmount+=bigoneday_leimu_itemAmount;
							
						 bigoneday_leimu_itemCount=Integer.parseInt(_cols[4]);
						 bigoneday_leimu_itemAmount=Double.parseDouble(_cols[5]);
						 bigoneday_leimu_itemTable.addCol(_cols[1]);
					 }
					
					
					
					bigoneday_leimu_itemTable.addCol(_cols[2]);
					bigoneday_leimu_itemTable.addCol(_cols[3]);
					bigoneday_leimu_itemTable.addCol(_cols[4]);
					bigoneday_leimu_itemTable.addCol(new BigDecimal(_cols[5]).toPlainString());
					bigoneday_leimu_itemTable.breakRow();
				}else if(smalloneday_leimu_item.equals(_cols[0])){
					if(""==smalloneday_leimu_itemPre){
						smalloneday_leimu_itemPre= _cols[1];
						smalloneday_leimu_itemCount=Integer.parseInt(_cols[4]);
						smalloneday_leimu_itemAmount=Double.parseDouble(_cols[5]);
						smalloneday_leimu_itemTable.addCol(_cols[1]);
					 }else if(_cols[1].equals(smalloneday_leimu_itemPre)){//相等 说明是同一个类目
						 smalloneday_leimu_itemCount+= Integer.parseInt(_cols[4]);
						 smalloneday_leimu_itemAmount+=Double.parseDouble(_cols[5]);
						 smalloneday_leimu_itemTable.addCol("--");
					 }
					 else{//不相等
						 smalloneday_leimu_itemTable.addCol(smalloneday_leimu_itemPre+"合计：");
						 smalloneday_leimu_itemTable.addCol("--");
						 smalloneday_leimu_itemTable.addCol("--");
						 smalloneday_leimu_itemTable.addCol(smalloneday_leimu_itemCount+"");
						 smalloneday_leimu_itemTable.addCol(smalloneday_leimu_itemAmount+"");
						 smalloneday_leimu_itemTable.breakRow();
						
						 smalloneday_leimu_itemPre= _cols[1];
						
						 smalloneday_leimu_itemTotalCount+=smalloneday_leimu_itemCount;
						 smalloneday_leimu_itemTotalAmount+=smalloneday_leimu_itemAmount;
							
						 smalloneday_leimu_itemCount=Integer.parseInt(_cols[4]);
						 smalloneday_leimu_itemAmount=Double.parseDouble(_cols[5]);
						 smalloneday_leimu_itemTable.addCol(_cols[1]);
					 }
					
					
					
					
					smalloneday_leimu_itemTable.addCol(_cols[2]);
					smalloneday_leimu_itemTable.addCol(_cols[3]);
					smalloneday_leimu_itemTable.addCol(_cols[4]);
					smalloneday_leimu_itemTable.addCol(new BigDecimal(_cols[5]).toPlainString());
					smalloneday_leimu_itemTable.breakRow();
				}
				//类目商品维度--------------------------结束
				else if(bigOneday_seller.equals(_cols[0])){
					
					bigOneday_sellerTable.addCol(_cols[1]);
					bigOneday_sellerTable.addCol(_cols[3]);
					bigOneday_sellerTable.addCol(new BigDecimal(_cols[2]).toPlainString());
					bigOneday_sellerTable.breakRow();
				}else if(bigOneyear_seller.equals(_cols[0])){
					
					bigOneyear_sellerTable.addCol(_cols[1]);
					bigOneyear_sellerTable.addCol(_cols[3]);
					bigOneyear_sellerTable.addCol(new BigDecimal(_cols[2]).toPlainString());
					bigOneyear_sellerTable.breakRow();
				}else if(smallOneday_seller.equals(_cols[0])){
					
					smallOneday_sellerTable.addCol(_cols[1]);
					smallOneday_sellerTable.addCol(_cols[3]);
					smallOneday_sellerTable.addCol(new BigDecimal(_cols[2]).toPlainString());
					smallOneday_sellerTable.breakRow();
				}else if(smallOneyear_seller.equals(_cols[0])){
					
					smallOneyear_sellerTable.addCol(_cols[1]);
					smallOneyear_sellerTable.addCol(_cols[3]);
					smallOneyear_sellerTable.addCol(new BigDecimal(_cols[2]).toPlainString());
					smallOneyear_sellerTable.breakRow();
				}
				//卖家维度----------------------------结束
			}
			
		}
		
		smallOneDay_leimu_amountTable.addCol(smallOneDay_leimu_amountPre+"合计：");
		smallOneDay_leimu_amountTable.addCol("--");
		smallOneDay_leimu_amountTable.addCol(smallOneDay_leimu_amountCount+"");
		smallOneDay_leimu_amountTable.addCol(smallOneDay_leimu_amountAmount.longValue()+"");
		smallOneDay_leimu_amountTable.breakRow();
		//总合计：
		smallOneDay_leimu_amountTable.addCol("全类目合计：");
		smallOneDay_leimu_amountTable.addCol("--");
		smallOneDay_leimu_amountTable.addCol(smallOneDay_leimu_amountTotalCount+smallOneDay_leimu_amountCount+"");
		smallOneDay_leimu_amountTable.addCol(smallOneDay_leimu_amountTotalAmount+smallOneDay_leimu_amountAmount+"");
		smallOneDay_leimu_amountTable.breakRow();
	//-----------------------------------------	
		
		
		bigOneDay_leimu_amountTable.addCol(bigOneDay_leimu_amountPre+"合计：");
		bigOneDay_leimu_amountTable.addCol("--");
		bigOneDay_leimu_amountTable.addCol(bigOneDay_leimu_amountCount+"");
		bigOneDay_leimu_amountTable.addCol(bigOneDay_leimu_amountAmount.longValue()+"");
		bigOneDay_leimu_amountTable.breakRow();
		//总合计：
		bigOneDay_leimu_amountTable.addCol("全类目合计：");
		bigOneDay_leimu_amountTable.addCol("--");
		bigOneDay_leimu_amountTable.addCol(bigOneDay_leimu_amountTotalCount+bigOneDay_leimu_amountCount+"");
		bigOneDay_leimu_amountTable.addCol(bigOneDay_leimu_amountTotalAmount+bigOneDay_leimu_amountAmount+"");
		bigOneDay_leimu_amountTable.breakRow();
				
			
		smallOneYear_leimu_amountTable.addCol(smallOneYear_leimu_amountPre+"合计：");
		smallOneYear_leimu_amountTable.addCol("--");
		smallOneYear_leimu_amountTable.addCol(smallOneYear_leimu_amountCount+"");
		smallOneYear_leimu_amountTable.addCol(smallOneYear_leimu_amountAmount.longValue()+"");
		smallOneYear_leimu_amountTable.breakRow();
		//总合计：
		smallOneYear_leimu_amountTable.addCol("全类目合计：");
		smallOneYear_leimu_amountTable.addCol("--");
		smallOneYear_leimu_amountTable.addCol(smallOneYear_leimu_amountTotalCount+smallOneYear_leimu_amountCount+"");
		smallOneYear_leimu_amountTable.addCol(smallOneYear_leimu_amountTotalAmount+smallOneYear_leimu_amountAmount+"");
		smallOneYear_leimu_amountTable.breakRow();
		
		
		
		bigOneYear_leimu_amountTable.addCol(bigOneYear_leimu_amountPre+"合计：");
		bigOneYear_leimu_amountTable.addCol("--");
		bigOneYear_leimu_amountTable.addCol(bigOneYear_leimu_amountCount+"");
		bigOneYear_leimu_amountTable.addCol(bigOneYear_leimu_amountAmount.longValue()+"");
		bigOneYear_leimu_amountTable.breakRow();
		//总合计：
		bigOneYear_leimu_amountTable.addCol("全类目合计：");
		bigOneYear_leimu_amountTable.addCol("--");
		bigOneYear_leimu_amountTable.addCol(bigOneYear_leimu_amountTotalCount+bigOneYear_leimu_amountCount+"");
		bigOneYear_leimu_amountTable.addCol(new BigDecimal(bigOneYear_leimu_amountTotalAmount+bigOneYear_leimu_amountAmount+"").toPlainString());
		bigOneYear_leimu_amountTable.breakRow();
		
		
		
		smalloneday_leimu_itemTable.addCol(smalloneday_leimu_itemPre+"合计：");
		smalloneday_leimu_itemTable.addCol("--");
		 smalloneday_leimu_itemTable.addCol("--");
		smalloneday_leimu_itemTable.addCol(smalloneday_leimu_itemCount+"");
		smalloneday_leimu_itemTable.addCol(smalloneday_leimu_itemAmount+"");
		smalloneday_leimu_itemTable.breakRow();
		
		//总合计：
		smalloneday_leimu_itemTable.addCol("全类目合计：");
		smalloneday_leimu_itemTable.addCol("--");
		smalloneday_leimu_itemTable.addCol("--");
		smalloneday_leimu_itemTable.addCol(smalloneday_leimu_itemTotalCount+smalloneday_leimu_itemCount+"");
		smalloneday_leimu_itemTable.addCol(new BigDecimal(smalloneday_leimu_itemTotalAmount+smalloneday_leimu_itemAmount+"").toPlainString());
		smalloneday_leimu_itemTable.breakRow();
		
		
		bigoneday_leimu_itemTable.addCol(bigoneday_leimu_itemPre+"合计：");
		bigoneday_leimu_itemTable.addCol("--");
		 bigoneday_leimu_itemTable.addCol("--");
		bigoneday_leimu_itemTable.addCol(bigoneday_leimu_itemCount+"");
		bigoneday_leimu_itemTable.addCol(bigoneday_leimu_itemAmount+"");
		bigoneday_leimu_itemTable.breakRow();
		
		//总合计：
		bigoneday_leimu_itemTable.addCol("全类目合计：");
		bigoneday_leimu_itemTable.addCol("--");
		bigoneday_leimu_itemTable.addCol("--");
		bigoneday_leimu_itemTable.addCol(bigoneday_leimu_itemTotalCount+bigoneday_leimu_itemCount+"");
		bigoneday_leimu_itemTable.addCol(new BigDecimal(bigoneday_leimu_itemTotalAmount+bigoneday_leimu_itemAmount+"").toPlainString());
		bigoneday_leimu_itemTable.breakRow();
		
		
		XmlReportFactory.dump(report, new FileOutputStream(input + ".xml"));	

	}

}
