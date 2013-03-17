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
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

/** 
 * @ClassName: FangCount 
 * @Description: TODO(������һ�仰��������������) 
 * @author tiance
 * @date 2011-11-16 ����11:02:18 
 * @version 1.0 
 */
public class FangCount {
	
	private static char CTRL_A = (char) 0x01;
	/** 
	 *
	 * @Title: main 
	 * @Description: TODO(������һ�仰�����������������) 
	 * @param @param args    �趨�ļ� 
	 * @return void    �������� 
	 * @throws 
	 */
	public static void main(String[] args)   throws IOException{
//		String input    = "d:\\20111202";      //�ļ�·��
//      String mainName ="�Ա�����";      //��������
        String input    = args[0];      //�ļ�·��
        String mainName = args[1];      //��������
        String tableName = "fangpvuv";
        String tableTitle="PVUV";
        
        String zufang="zufang",ershoufang="ershoufang",all="all",detail="detail",qmfq="qmfq",alluv="unrepeatuv";
        
        
        if (!new File(input).exists()) {
            System.out.println("File Not Exist ! => " + input);
            return;
        }
        Report report = Report.newReport(mainName + "ÿ��PV��UV����");
        List<String> lines = Utils.readWithCharset(input, "utf-8");
        Table table = null;
        int allPV = 0;
        if(lines != null && lines.size()>0){
        	for(String line:lines){
        		if(line.startsWith(zufang)){
        			tableName = "zufangpvuv";
        			tableTitle  = "�ⷿƵ������PV��UV";
        		}else if(line.startsWith(ershoufang)){
        			tableName = "ershoufangpvuv";
        			tableTitle  = "���ַ�Ƶ������PV��UV";
        		}else if(line.startsWith(all)){
        			tableName = "houseall";
        			tableTitle  = "House�µ���PV��UV";
        		}else if(line.startsWith(detail)){
        			tableName = "detailpvuv";
        			tableTitle  = "Detail����PV��UV";
        		}else if(line.startsWith(qmfq)){
        			tableName = "qmfqpvuv";
        			tableTitle  = "ȫ���������PV��UV";
        		}else if(line.startsWith(alluv)){
        			tableName = "allpvuv";
        			tableTitle  = "��վȥ��UV";
        		}
        		String[] _cols = StringUtils.splitPreserveAllTokens(line, CTRL_A);
        		if(_cols.length>0){
        			if(tableName.equals("allpvuv")){
        				table = report.newTable(tableName, mainName + "��PV��UV(ȥ��)");
        				table.addCol("allpv", "��PV",String.valueOf(allPV));
        				table.addCol("vpv", "�ÿ�UV",_cols[1]);
        				table.addCol("uuv", "��ԱUV",_cols[2]);
        				table.addCol("alluv", "��UV",String.valueOf(Integer.valueOf(_cols[1])+Integer.valueOf(_cols[2])));
        				break;
        			}else{
        				table =  report.newTable(tableName, mainName + tableTitle);
        				table.addCol("vpv", "�ÿ�PV",_cols[1]);
        				table.addCol("upv", "��ԱPV",_cols[2]);
        				if(!tableName.equals("qmfqpvuv") &&!tableName.equals("alluv")  ){
        					allPV = allPV+Integer.valueOf(_cols[1])+Integer.valueOf(_cols[2]);
        				}
        				table.addCol("vuv", "�ÿ�UV",_cols[3]);
        				table.addCol("uuv", "��ԱUV",_cols[4]);
        			}
        		}
        		table.sort(Table.SORT_VALUE);
        	}
        }
        XmlReportFactory.dump(report, new FileOutputStream(input + ".xml"));
	}
	
	
	
	
}
