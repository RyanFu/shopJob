package org.dueam.hadoop.bp.report.life;

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
 * 
 * @author:feiyue
 * @email:feiyue@taobao.com
 * @date:2012-11-20
 * @description: 解晰sql返回结果生成xml文件，供报表使用。预约产品
 */
public class YuYueReport {

	private static char CTRL_A=(char)0x01; //分隔符
	static String mainName="本地生活-预约产品"; //报表主名称;
	
	/**
	 * 报表内容：
	 * 按类目统计当天的成交金额，成交笔数，卖家数，买家数，宝贝数
	 * 
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		String input=args[0]; //文件路径
		//String input = "D:/20130227";
		
		Report report = Report.newReport(mainName+"交易数据");
		if (!new File(input).exists()) {
			System.out.println("File Not Exist !" + input);
			return;
		}
		List<String> lines = Utils.readWithCharset(input, "utf-8");
		String[] yuyue_titleLines = new String[]{"预约金额","预约笔数","卖家数","买家数","宝贝数"};
		String[] yuyue_titlesum = new String[]{"预约金额总计","预约笔数总计","卖家数总计","买家数总计","预约宝贝数总计"};
		String[] money_titleLines = new String[]{"打款金额","打款笔数","卖家数","买家数","宝贝数"};
        String[] money_titlesum = new String[]{"打款金额总计","打款笔数总计","卖家数总计","买家数总计","宝贝数总计"};
		if (lines!= null && lines.size() > 0) {	
			generationStatisticsTable("类目名称","categoryStatistics_yuyue","按类目统计当天预约数", lines,report,yuyue_titleLines,yuyue_titlesum);
	        generationStatisticsTable("类目名称","categoryStatistics_money","按类目统计当天打款额", lines,report,money_titleLines,money_titlesum);
			generationStatisticsTable("月份","categoryStatistics_yuyue_month_2013","2013年按月份统计预约数", lines,report,yuyue_titleLines,yuyue_titlesum);
            generationStatisticsTable("月份","categoryStatistics_money_month_2013","2013年按月份统计打款额", lines,report,money_titleLines,money_titlesum);
			generationStatisticsTable("月份","categoryStatistics_yuyue_month_2012","2012年按月份统计预约数", lines,report,yuyue_titleLines,yuyue_titlesum);
			generationStatisticsTable("月份","categoryStatistics_money_month_2012","2012年按月份统计打款额", lines,report,money_titleLines,money_titlesum);
		}
		XmlReportFactory.dump(report, new FileOutputStream(input + ".xml"));
	}	
   
  /**
   *  按类目执行统计代码
   * @param dimension 统计维度
   * @param catgoryStatistics 参加统计的数据行
   * @param tableTitile 报表标题
   * @param lines
   * @param report
   */
    private static void generationStatisticsTable(String dimension,String catgoryStatistics,String tableTitile,List<String> lines,
            Report report,String[] titleLines,String[] titlesum) {
        double catgoryStatistics_money_sum = 0; //成交金额总计
        int catgoryStatistics_num = 0; //成交笔数总计
        int catgoryStatistics_seller_num = 0; //有交易的卖家总计
        int catgoryStatistics_buy_num = 0; //有交易的买家总计
        int  catgoryStatistics_item_num = 0; //有交易的宝贝数
        
        Table catgoryStatisticsTable = report.newViewTable(catgoryStatistics, mainName + tableTitile);
        catgoryStatisticsTable.addCol(dimension).addCol(titleLines[0]).addCol(titleLines[1])
                                .addCol(titleLines[2]).addCol(titleLines[3]).addCol(titleLines[4]).addCol(Report.BREAK_VALUE);
        for(String line:lines){
            if(line == null || "".equals(line)){
                continue;
            }
            String[] _cols = StringUtils.split(line,CTRL_A);
            if(catgoryStatistics.equals(_cols[0])){
                catgoryStatisticsTable.addCol(_cols[1]);
                catgoryStatisticsTable.addCol(_cols[1]+"成交金额",_cols[2]);
                catgoryStatisticsTable.addCol(_cols[1]+"成交笔数",_cols[3]);
                catgoryStatisticsTable.addCol(_cols[1]+"卖家数",_cols[4]);
                catgoryStatisticsTable.addCol(_cols[1]+"买家数",_cols[5]);
                catgoryStatisticsTable.addCol(_cols[1]+"宝贝数",_cols[6]);
                catgoryStatisticsTable.breakRow();
                
                catgoryStatistics_money_sum += Double.parseDouble(_cols[2]);
                catgoryStatistics_num += Integer.parseInt(_cols[3]);
                catgoryStatistics_seller_num += Integer.parseInt(_cols[4]);
                catgoryStatistics_buy_num += Integer.parseInt(_cols[5]); 
                catgoryStatistics_item_num += Integer.parseInt(_cols[6]);
            }
        }
        
        catgoryStatisticsTable.addCol("总计");
        catgoryStatisticsTable.addCol(titlesum[0],new BigDecimal(Double.toString(catgoryStatistics_money_sum)).toString());
        catgoryStatisticsTable.addCol(titlesum[1],String.valueOf(catgoryStatistics_num));
        catgoryStatisticsTable.addCol(titlesum[2],String.valueOf(catgoryStatistics_seller_num));
        catgoryStatisticsTable.addCol(titlesum[3],String.valueOf(catgoryStatistics_buy_num));
        catgoryStatisticsTable.addCol(titlesum[4],String.valueOf(catgoryStatistics_item_num));
        catgoryStatisticsTable.breakRow();
    }
}



