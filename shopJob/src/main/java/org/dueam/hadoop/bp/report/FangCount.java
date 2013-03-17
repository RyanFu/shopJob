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
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

/** 
 * @ClassName: FangCount 
 * @Description: TODO(这里用一句话描述这个类的作用) 
 * @author tiance
 * @date 2011-11-16 上午11:02:18 
 * @version 1.0 
 */
public class FangCount {
	
	private static char CTRL_A = (char) 0x01;
	/** 
	 *
	 * @Title: main 
	 * @Description: TODO(这里用一句话描述这个方法的作用) 
	 * @param @param args    设定文件 
	 * @return void    返回类型 
	 * @throws 
	 */
	public static void main(String[] args)   throws IOException{
//		String input    = "d:\\20111202";      //文件路径
//      String mainName ="淘宝房产";      //报表名称
        String input    = args[0];      //文件路径
        String mainName = args[1];      //报表名称
        String tableName = "fangpvuv";
        String tableTitle="PVUV";
        
        String zufang="zufang",ershoufang="ershoufang",all="all",detail="detail",qmfq="qmfq",alluv="unrepeatuv";
        
        
        if (!new File(input).exists()) {
            System.out.println("File Not Exist ! => " + input);
            return;
        }
        Report report = Report.newReport(mainName + "每日PV、UV数据");
        List<String> lines = Utils.readWithCharset(input, "utf-8");
        Table table = null;
        int allPV = 0;
        if(lines != null && lines.size()>0){
        	for(String line:lines){
        		if(line.startsWith(zufang)){
        			tableName = "zufangpvuv";
        			tableTitle  = "租房频道当日PV、UV";
        		}else if(line.startsWith(ershoufang)){
        			tableName = "ershoufangpvuv";
        			tableTitle  = "二手房频道当日PV、UV";
        		}else if(line.startsWith(all)){
        			tableName = "houseall";
        			tableTitle  = "House下当日PV、UV";
        		}else if(line.startsWith(detail)){
        			tableName = "detailpvuv";
        			tableTitle  = "Detail当日PV、UV";
        		}else if(line.startsWith(qmfq)){
        			tableName = "qmfqpvuv";
        			tableTitle  = "全民疯抢当日PV、UV";
        		}else if(line.startsWith(alluv)){
        			tableName = "allpvuv";
        			tableTitle  = "整站去重UV";
        		}
        		String[] _cols = StringUtils.splitPreserveAllTokens(line, CTRL_A);
        		if(_cols.length>0){
        			if(tableName.equals("allpvuv")){
        				table = report.newTable(tableName, mainName + "总PV、UV(去重)");
        				table.addCol("allpv", "总PV",String.valueOf(allPV));
        				table.addCol("vpv", "访客UV",_cols[1]);
        				table.addCol("uuv", "会员UV",_cols[2]);
        				table.addCol("alluv", "总UV",String.valueOf(Integer.valueOf(_cols[1])+Integer.valueOf(_cols[2])));
        				break;
        			}else{
        				table =  report.newTable(tableName, mainName + tableTitle);
        				table.addCol("vpv", "访客PV",_cols[1]);
        				table.addCol("upv", "会员PV",_cols[2]);
        				if(!tableName.equals("qmfqpvuv") &&!tableName.equals("alluv")  ){
        					allPV = allPV+Integer.valueOf(_cols[1])+Integer.valueOf(_cols[2]);
        				}
        				table.addCol("vuv", "访客UV",_cols[3]);
        				table.addCol("uuv", "会员UV",_cols[4]);
        			}
        		}
        		table.sort(Table.SORT_VALUE);
        	}
        }
        XmlReportFactory.dump(report, new FileOutputStream(input + ".xml"));
	}
	
	
	
	
}
