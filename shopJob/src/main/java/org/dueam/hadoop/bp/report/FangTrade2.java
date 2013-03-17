/**
 * 
 */
package org.dueam.hadoop.bp.report;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.dueam.hadoop.common.util.MapUtils;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;
/**
 * @author longjia.zt
 *
 */
public class FangTrade2 {
	private static char CTRL_A = (char) 0x01;
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		    //String input = "C:\\Users\\longjia.zt\\Documents\\20120222"; // �ļ�·��
				String mainName = "�Ա�����"; // ��������
			 	String input = args[0]; // �ļ�·��
				// String mainName = args[1]; //��������

				if (!new File(input).exists()) {
					System.out.println("File Not Exist ! => " + input);
					return;
				}
				Report report = Report.newReport(mainName + "���ױ���");
				//�����ܵ�
				String ALLTRADES = "alltrades";//���н��ױ���
				String ALLTRADESGMV = "alltradesGMV";//���н���GMV
				String TRADEUSER = "tradeUser";//���еĽ�����  ȥ�أ�
				String ALLSUCCESSTRADE = "allsuccesstrade";//���׳ɹ�����
				String ALLSUCCESSM = "allsuccessM";//���׳ɹ����
				Table totalGMV = report.newTable("totalGMV",
						"�Ա����������ܼ�", "γ��", "����");
		
				String LEIMUM="leimuM";
				Table leimuSuccessM = report.newGroupTable("leimuSuccessM",
						"������Ŀ�ѳɹ�����ͳ��", "γ��", "���");
				
				String LEIMUNUM="leimuNum";
				Table leimuSuccessMUM = report.newGroupTable("leimuSuccessMUM",
						"������Ŀ�ѳɹ����ױ���ͳ��", "γ��", "����");
				
				
				String LEIMUBUYNUM="leimubuyNum";
				Table leimuSuccessBuyUser = report.newGroupTable("leimuSuccessBuyUser",
						"������Ŀ�ѳɹ���������ͳ��", "γ��", "����");

				String LEIMUGMV="leimuGMV";
				Table leimuGMV = report.newGroupTable("leimuGMV",
						"������ĿGMV����ͳ��", "γ��", "���");
				
				List<String> lines = Utils.readWithCharset(input, "utf-8");
				// ��ѭ����
				if (lines != null && lines.size() > 0) {
					long tradeUsers = 0; 
					//ͳ��ÿ����Ŀ���������
					int others=0;//50023902
					int zhongjie=0;//50023579
					int chuzhu=0;//50023597
					int xinfang=0;//50023944
					int ershoufang=0;//50023598
					
					for (String line : lines) {
						String[] _cols = StringUtils.splitPreserveAllTokens(line,
								CTRL_A);
						if (ALLTRADES.equals(_cols[0])) {
							totalGMV.addCol(Report.newValue("GMV���ױ���ͳ��",_cols[1]));

						}// 
						
						else if (ALLTRADESGMV.equals(_cols[0])) { 
							totalGMV.addCol(Report.newValue("GMV���׽��ͳ��",_cols[1]));

						}// 
						else if (TRADEUSER.equals(_cols[0])) { 
							tradeUsers=tradeUsers+1;

						}
						else if (ALLSUCCESSTRADE.equals(_cols[0])) {// ˵������Ʒ���� ����״̬ά�ȵ�
							totalGMV.addCol(Report.newValue("�ѳɹ����ױ���ͳ��",_cols[1]));

						}//
						
						else if (ALLSUCCESSM.equals(_cols[0])) {// ˵������Ʒ���� ����״̬ά�ȵ�
							totalGMV.addCol(Report.newValue("�ѳɹ����׽��ͳ��",_cols[1]));

						}//
						
						else if (LEIMUM.equals(_cols[0])) {// ˵������Ʒ���� ����״̬ά�ȵ�
							leimuSuccessM.addCol(Report.newValue(FangAACount.getLeiMuNale(_cols[1]),_cols[2]));
						}//
						
						else if (LEIMUNUM.equals(_cols[0])) {// ˵������Ʒ���� ����״̬ά�ȵ�
							leimuSuccessMUM.addCol(Report.newValue(FangAACount.getLeiMuNale(_cols[1]),_cols[2]));
						}//
						
						else if (LEIMUBUYNUM.equals(_cols[0])) {// ˵������Ʒ���� ����״̬ά�ȵ�
							
							if("50023902".equals(_cols[1])){
								others=others+1;
							}
							else if("50023579".equals(_cols[1])){
								zhongjie=zhongjie+1;
							}
							else if("50023597".equals(_cols[1])){
								chuzhu=chuzhu+1;
							}
							else if("50023944".equals(_cols[1])){
								xinfang=xinfang+1;
							}
							else if("50023598".equals(_cols[1])){
								ershoufang=ershoufang+1;
							}
							
							
							
						}//
						
						else if (LEIMUGMV.equals(_cols[0])) {// ˵������Ʒ���� ����״̬ά�ȵ�
							leimuGMV.addCol(Report.newValue(FangAACount.getLeiMuNale(_cols[1]),_cols[2]));
						}//
						
					}// for

				totalGMV.addCol(Report.newValue("�����뽻������ͳ��",tradeUsers+""));
				leimuSuccessBuyUser.addCol(Report.newValue("�ⷿ/����/����/�������",chuzhu+""));
				leimuSuccessBuyUser.addCol(Report.newValue("���ַ�/����/����",ershoufang+""));
				leimuSuccessBuyUser.addCol(Report.newValue("�·�/��¥��/һ�ַ�",xinfang+""));
				leimuSuccessBuyUser.addCol(Report.newValue("ί�з���/�н����",zhongjie+""));
				leimuSuccessBuyUser.addCol(Report.newValue("������������",others+""));
				}// if
				XmlReportFactory.dump(report, new FileOutputStream(input + ".xml"));

			}


}
