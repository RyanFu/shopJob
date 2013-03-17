/**   
 *********************
 *                                  * 
 *   淘宝房产，版权所有！  *
 *                                  * 
 *     fang.taobao.com       *
 *********************
* @Title: FangCount.java 
* @Package org.dueam.hadoop.bp.report 
* @Description: TODO(用一句话描述该文件做什么) 
* @author tiance
* @date 2011-11-16 上午11:02:18 
* @version V1.0   
*/
package org.dueam.hadoop.bp.report;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

/** 
 * @ClassName: FangCountBigPay 
 * @Description: TODO(这里用一句话描述这个类的作用) 
 * @author longjia
 * @date 2011-11-16 上午11:02:18 
 * @version 1.0 
 */
public class FangCountBigPay {
	
	private static char CTRL_A = (char) 0x01;
	/**
	 * @throws ParseException  
	 *
	 * @Title: main 
	 * @Description: TODO(这里用一句话描述这个方法的作用) 
	 * @param @param args    设定文件 
	 * @return void    返回类型 
	 * @throws 
	 */
	public static void main(String[] args)   throws IOException, ParseException{
		String input    = "d:\\20120304";      //文件路径
        String mainName ="淘宝房产";      //报表名称
       // String input    = args[0];      //文件路径
        
        if (!new File(input).exists()) {
            System.out.println("File Not Exist ! => " + input);
            return;
        }
        Calendar now = Calendar.getInstance();//使用默认时区和语言环境获得一个日历。   
        now.add(Calendar.MONTH, -1);//取当前日期的前一天.
        Calendar dayMouthEnd = new GregorianCalendar(now.get(Calendar.YEAR), now.get(Calendar.MONTH), now.getActualMaximum(Calendar.DAY_OF_MONTH),23,59,59);
        Calendar dayMouthStart = new GregorianCalendar(now.get(Calendar.YEAR), now.get(Calendar.MONTH), 1,0,0,0);    
        Report report = Report.newReport(mainName + "每月大定统计");
        Table table = report.newViewTable("",  "淘宝房产大定成交排行榜");
        table.addCol("商品ID").addCol("卖家用户名").addCol("支付宝成交金额").addCol("支付时间").addCol(Report.BREAK_VALUE);        
		List<String> lines = Utils.readWithCharset(input, "utf-8");
        //先统计用户打表
		double bigPrice=0;//总和
        if(lines != null && lines.size()>0){ 
        	for(String line:lines){
        		String[] _cols = StringUtils.splitPreserveAllTokens(line, CTRL_A);
        		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        		Date payTime=sdf.parse(_cols[4]);
        		if(_cols.length>0&&(dayMouthStart.getTime().getTime()<=payTime.getTime())&&(payTime.getTime()<=dayMouthEnd.getTime().getTime())){
        		table.addCol(_cols[0]).addCol(_cols[2]).addCol(_cols[3]).addCol(_cols[4]).breakRow();
        		bigPrice=bigPrice+Double.parseDouble(_cols[3]);
        		}
 
        	}
        	
        }
        table.addCol("总计：").addCol(bigPrice+"");
        XmlReportFactory.dump(report, new FileOutputStream(input + ".xml"));
	}	
}
