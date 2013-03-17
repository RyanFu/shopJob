/**
 * 
 */
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
 * @author longjia.zt
 *
 */
public class OfflineTradeOneWeekOneDay {
	private static char CTRL_A=(char)0x01; //分隔符
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		//String input = "D:\\20121011"; // 文件路径
		String mainName="线下支付"; //报表主名称
		String input=args[0]; //文件路径
		//类目成交金额维度
		String bigOneDay_leimu_amount="bigOneDay_leimu_amount"; //
		String smallOneDay_leimu_amount="smallOneDay_leimu_amount"; // 

		Report report = Report.newReport(mainName + "上周五到本周四交易数据统计");

		//类目成交金额维度-------------------------------
		Table smallOneDay_leimu_amountTable = report.newViewTable(smallOneDay_leimu_amount, mainName+"本周小定定类目成交");
		smallOneDay_leimu_amountTable.addCol("主类目").addCol("叶子类目").addCol("成交笔数").addCol("成交金额").addCol(Report.BREAK_VALUE);;
		
		
		Table bigOneDay_leimu_amountTable = report.newViewTable(bigOneDay_leimu_amount, mainName+"本周大定类目成交");
		bigOneDay_leimu_amountTable.addCol("主类目").addCol("叶子类目").addCol("成交笔数").addCol("成交金额").addCol(Report.BREAK_VALUE);;
		
		
		//类目成交金额维度-------------------------------结束
		
		

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
				}
				//类目成交金额维度-------------------------------结束
				
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
		
		XmlReportFactory.dump(report, new FileOutputStream(input + ".xml"));	

	}

}
