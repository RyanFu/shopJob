package org.dueam.hadoop.bp.report.fang;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

public class DianyinPUV_Refer {

	public static final char CTRL_A = (char) 0x01;
	public static final  String piao="piao";
	public static final  String ticket="ticket";
	public static final String dianyin="dianying";
	public static final String y_piao="y.piao";
	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		 //String input = "C:\\Users\\longjia.zt\\Documents\\20120827"; // �ļ�·��
		 String mainName = "��ӰƱ"; // ��������
	 	  String input = args[0]; // �ļ�·��
		if (!new File(input).exists()) {
			System.out.println("File Not Exist ! => " + input);
			return;
		}
		
		Report report = Report.newReport(mainName + "��ֱ��Ŀ����ͳ��");

		
		Table pv = report.newGroupTable("�Ա���Ӱ��PV","�Ա���Ӱ��PVͳ��", "γ��", "����");
		
		Table uv = report.newGroupTable("�Ա���Ӱ��UV","�Ա���Ӱ��UVͳ��", "γ��", "����");
		
		Table dianyinrefer=report.newViewTable("dianyinrefer", mainName+ "dianyin.taobao.com��Դǰ25");
		dianyinrefer.addCol("��Դ��������").addCol("PV����").addCol("UV����").addCol(Report.BREAK_VALUE);
		
		
		Table piaorefer=report.newViewTable("piaorefer", mainName+ "piao.taobao.com��Դǰ25");
		piaorefer.addCol("��Դ��������").addCol("PV����").addCol("UV����").addCol(Report.BREAK_VALUE);
		
		Table y_piaorefer=report.newViewTable("y_piaorefer", mainName+ "y.piao.taobao.com��Դǰ25");
		y_piaorefer.addCol("��Դ��������").addCol("PV����").addCol("UV����").addCol(Report.BREAK_VALUE);
		
		Table ticketrefer=report.newViewTable("ticketrefer", mainName+"ticket.taobao.com��Դǰ25");
		ticketrefer.addCol("��Դ��������").addCol("PV����").addCol("UV����").addCol(Report.BREAK_VALUE);
		
		
		List<String> lines = Utils.readWithCharset(input, "utf-8");
		if (lines != null && lines.size() > 0) {
			for (String line : lines) {
				String[] _cols = StringUtils.splitPreserveAllTokens(line,CTRL_A);
				if ((piao+"pvuv").equals(_cols[0])) { 				
					uv.addCol(Report.newValue("piao.taobao.com UV:", _cols[2]));
					pv.addCol(Report.newValue("piao.taobao.com PV:", _cols[1]));
			
				} 
				else if ((y_piao+"pvuv").equals(_cols[0])) { 				
					uv.addCol(Report.newValue("y.piao.taobao.com UV:", _cols[2]));
					pv.addCol(Report.newValue("y.piao.taobao.com PV:", _cols[1]));
			
				} 
				else if ((dianyin+"pvuv").equals(_cols[0])) { 				
					uv.addCol(Report.newValue("dianyin.taobao.com UV:", _cols[2]));
					pv.addCol(Report.newValue("dianyin.taobao.com PV:", _cols[1]));
		
				} 
				else if ((ticket+"pvuv").equals(_cols[0])) { 				
					uv.addCol(Report.newValue("ticket.taobao.com UV:", _cols[2]));
					pv.addCol(Report.newValue("ticket.taobao.com PV:", _cols[1]));
				}
				else if ((piao+"_refer").equals(_cols[0])) { 
					if(_cols[1]==null||_cols[1]==""||_cols[1].length()==0){
						_cols[1]="����Դ";
					}
					piaorefer.addCol(_cols[1]);
					piaorefer.addCol(_cols[1]+"pv", _cols[2]);
					piaorefer.addCol(_cols[1]+"uv", _cols[3]);
					piaorefer.breakRow();
				} 
				else if ((y_piao+"_refer").equals(_cols[0])) { 
					if(_cols[1]==null||_cols[1]==""||_cols[1].length()==0){
						_cols[1]="����Դ";
					}
					y_piaorefer.addCol(_cols[1]);
					y_piaorefer.addCol(_cols[1]+"pv", _cols[2]);
					y_piaorefer.addCol(_cols[1]+"uv", _cols[3]);
					y_piaorefer.breakRow();
				} 
				else if ((dianyin+"_refer").equals(_cols[0])) { 
					if(_cols[1]==null||_cols[1]==""||_cols[1].length()==0){
						_cols[1]="����Դ";
					}
					dianyinrefer.addCol(_cols[1]);
					dianyinrefer.addCol(_cols[1]+"pv", _cols[2]);
					dianyinrefer.addCol(_cols[1]+"uv", _cols[3]);
					dianyinrefer.breakRow();
				} 
				else if ((ticket+"_refer").equals(_cols[0])) { 
					if(_cols[1]==null||_cols[1]==""||_cols[1].length()==0){
						_cols[1]="����Դ";
					}
					ticketrefer.addCol(_cols[1]);
					ticketrefer.addCol(_cols[1]+"pv", _cols[2]);
					ticketrefer.addCol(_cols[1]+"uv", _cols[3]);
					ticketrefer.breakRow();
				}
				
			}//for			
			
		}//if
		XmlReportFactory.dump(report, new FileOutputStream(input + ".xml"));

	}

}
