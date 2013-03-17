/**   
 *********************
 *                                  * 
 *   �Ա���������Ȩ���У�  *
 *                                  * 
 *     fang.taobao.com       *
 *********************
* @Title: FangCount.java 
* @Package org.dueam.hadoop.bp.report 
* @Description: TODO(��һ�仰�������ļ���ʲô) 
* @author tiance
* @date 2011-11-16 ����11:02:18 
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
 * @Description: TODO(������һ�仰��������������) 
 * @author longjia
 * @date 2011-11-16 ����11:02:18 
 * @version 1.0 
 */
public class FangCountBigPay {
	
	private static char CTRL_A = (char) 0x01;
	/**
	 * @throws ParseException  
	 *
	 * @Title: main 
	 * @Description: TODO(������һ�仰�����������������) 
	 * @param @param args    �趨�ļ� 
	 * @return void    �������� 
	 * @throws 
	 */
	public static void main(String[] args)   throws IOException, ParseException{
		String input    = "d:\\20120304";      //�ļ�·��
        String mainName ="�Ա�����";      //��������
       // String input    = args[0];      //�ļ�·��
        
        if (!new File(input).exists()) {
            System.out.println("File Not Exist ! => " + input);
            return;
        }
        Calendar now = Calendar.getInstance();//ʹ��Ĭ��ʱ�������Ի������һ��������   
        now.add(Calendar.MONTH, -1);//ȡ��ǰ���ڵ�ǰһ��.
        Calendar dayMouthEnd = new GregorianCalendar(now.get(Calendar.YEAR), now.get(Calendar.MONTH), now.getActualMaximum(Calendar.DAY_OF_MONTH),23,59,59);
        Calendar dayMouthStart = new GregorianCalendar(now.get(Calendar.YEAR), now.get(Calendar.MONTH), 1,0,0,0);    
        Report report = Report.newReport(mainName + "ÿ�´�ͳ��");
        Table table = report.newViewTable("",  "�Ա������󶨳ɽ����а�");
        table.addCol("��ƷID").addCol("�����û���").addCol("֧�����ɽ����").addCol("֧��ʱ��").addCol(Report.BREAK_VALUE);        
		List<String> lines = Utils.readWithCharset(input, "utf-8");
        //��ͳ���û����
		double bigPrice=0;//�ܺ�
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
        table.addCol("�ܼƣ�").addCol(bigPrice+"");
        XmlReportFactory.dump(report, new FileOutputStream(input + ".xml"));
	}	
}
