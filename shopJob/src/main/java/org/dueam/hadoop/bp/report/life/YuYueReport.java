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
 * @description: ����sql���ؽ������xml�ļ���������ʹ�á�ԤԼ��Ʒ
 */
public class YuYueReport {

	private static char CTRL_A=(char)0x01; //�ָ���
	static String mainName="��������-ԤԼ��Ʒ"; //����������;
	
	/**
	 * �������ݣ�
	 * ����Ŀͳ�Ƶ���ĳɽ����ɽ����������������������������
	 * 
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		String input=args[0]; //�ļ�·��
		//String input = "D:/20130227";
		
		Report report = Report.newReport(mainName+"��������");
		if (!new File(input).exists()) {
			System.out.println("File Not Exist !" + input);
			return;
		}
		List<String> lines = Utils.readWithCharset(input, "utf-8");
		String[] yuyue_titleLines = new String[]{"ԤԼ���","ԤԼ����","������","�����","������"};
		String[] yuyue_titlesum = new String[]{"ԤԼ����ܼ�","ԤԼ�����ܼ�","�������ܼ�","������ܼ�","ԤԼ�������ܼ�"};
		String[] money_titleLines = new String[]{"�����","������","������","�����","������"};
        String[] money_titlesum = new String[]{"������ܼ�","�������ܼ�","�������ܼ�","������ܼ�","�������ܼ�"};
		if (lines!= null && lines.size() > 0) {	
			generationStatisticsTable("��Ŀ����","categoryStatistics_yuyue","����Ŀͳ�Ƶ���ԤԼ��", lines,report,yuyue_titleLines,yuyue_titlesum);
	        generationStatisticsTable("��Ŀ����","categoryStatistics_money","����Ŀͳ�Ƶ������", lines,report,money_titleLines,money_titlesum);
			generationStatisticsTable("�·�","categoryStatistics_yuyue_month_2013","2013�갴�·�ͳ��ԤԼ��", lines,report,yuyue_titleLines,yuyue_titlesum);
            generationStatisticsTable("�·�","categoryStatistics_money_month_2013","2013�갴�·�ͳ�ƴ���", lines,report,money_titleLines,money_titlesum);
			generationStatisticsTable("�·�","categoryStatistics_yuyue_month_2012","2012�갴�·�ͳ��ԤԼ��", lines,report,yuyue_titleLines,yuyue_titlesum);
			generationStatisticsTable("�·�","categoryStatistics_money_month_2012","2012�갴�·�ͳ�ƴ���", lines,report,money_titleLines,money_titlesum);
		}
		XmlReportFactory.dump(report, new FileOutputStream(input + ".xml"));
	}	
   
  /**
   *  ����Ŀִ��ͳ�ƴ���
   * @param dimension ͳ��ά��
   * @param catgoryStatistics �μ�ͳ�Ƶ�������
   * @param tableTitile �������
   * @param lines
   * @param report
   */
    private static void generationStatisticsTable(String dimension,String catgoryStatistics,String tableTitile,List<String> lines,
            Report report,String[] titleLines,String[] titlesum) {
        double catgoryStatistics_money_sum = 0; //�ɽ�����ܼ�
        int catgoryStatistics_num = 0; //�ɽ������ܼ�
        int catgoryStatistics_seller_num = 0; //�н��׵������ܼ�
        int catgoryStatistics_buy_num = 0; //�н��׵�����ܼ�
        int  catgoryStatistics_item_num = 0; //�н��׵ı�����
        
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
                catgoryStatisticsTable.addCol(_cols[1]+"�ɽ����",_cols[2]);
                catgoryStatisticsTable.addCol(_cols[1]+"�ɽ�����",_cols[3]);
                catgoryStatisticsTable.addCol(_cols[1]+"������",_cols[4]);
                catgoryStatisticsTable.addCol(_cols[1]+"�����",_cols[5]);
                catgoryStatisticsTable.addCol(_cols[1]+"������",_cols[6]);
                catgoryStatisticsTable.breakRow();
                
                catgoryStatistics_money_sum += Double.parseDouble(_cols[2]);
                catgoryStatistics_num += Integer.parseInt(_cols[3]);
                catgoryStatistics_seller_num += Integer.parseInt(_cols[4]);
                catgoryStatistics_buy_num += Integer.parseInt(_cols[5]); 
                catgoryStatistics_item_num += Integer.parseInt(_cols[6]);
            }
        }
        
        catgoryStatisticsTable.addCol("�ܼ�");
        catgoryStatisticsTable.addCol(titlesum[0],new BigDecimal(Double.toString(catgoryStatistics_money_sum)).toString());
        catgoryStatisticsTable.addCol(titlesum[1],String.valueOf(catgoryStatistics_num));
        catgoryStatisticsTable.addCol(titlesum[2],String.valueOf(catgoryStatistics_seller_num));
        catgoryStatisticsTable.addCol(titlesum[3],String.valueOf(catgoryStatistics_buy_num));
        catgoryStatisticsTable.addCol(titlesum[4],String.valueOf(catgoryStatistics_item_num));
        catgoryStatisticsTable.breakRow();
    }
}



