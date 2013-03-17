//===================================================================================
// Copyright (c) 2004-2012 by www.taobao.com, All rights reserved.
// 9F., ChuangYe building, 99# huaxing road, HangZhou, China
// 
// This software is the confidential and proprietary information of 
// Taobao.com, Inc. ("Confidential Information"). You shall not disclose 
// such Confidential Information and shall use it only in accordance 
// with the terms of the license agreement you entered into with Taobao.com, Inc.
//===================================================================================
// File name: NewCarOfflinePayMoney.java
// Author: longque.zs
// Date: 2012-9-18 ����07:35:37 
// Description: 	 
// 		��
// Function List: 	 
// 		1. ��
// History: 
// 		1. ��
//===================================================================================

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
 * ����֧��-�����³���Ŀposˢ�����(���������³�������Ŀ(50050565)�������³�ȫ����Ŀ(50024973)�������³�����(50018718))
 * @author longque.zs
 * @version 1.0
 **/

public class NewCarOfflinePayMoney {
	
	private static char CTRL_A=(char)0x01; //�ָ���

	/**
	 * ��Ҫ���ݣ�
	 * �����³���Ŀ����С���ɽ����а�(ÿ��)�������û����ͱ���id����  ɸѡ���ݣ������û�������Ʒid���ɽ�������֧�����ɽ����
	 * �����³���Ŀ����POS�󶨳ɽ����а�(ÿ��)�������û����ͱ���id����  ɸѡ���ݣ������û�������Ʒid���ɽ�������֧�����ɽ����
	 * �����³���Ŀ2012������С���ɽ����а�(ÿ��)�������û�������  ɸѡ���ݣ������û������ɽ�������֧�����ɽ����
	 * �����³���Ŀ2012������POS�󶨳ɽ����а�(ÿ��)�������û�������  ɸѡ���ݣ������û������ɽ�������֧�����ɽ����
	 * @author longque.zs
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args)throws IOException{
//		String input = "D:\\usr\\20121029"; // �ļ�·��
		String mainName="����֧��-�����³�"; //����������
		String input=args[0]; //�ļ�·��
		Report report = Report.newReport(mainName+"��������");
		String smallOrder_Perday="smallOrderPerday"; //ÿ��С��
		String bigOrder_Perday="bigOrderPerday"; //ÿ���
		String smallOrder2012="smallOrder2012"; //2012С��
		String bigOrder2012="bigOrder2012"; //2012��
		String smallOrder2013="smallOrder2013"; //2013С��
		String bigOrder2013="bigOrder2013"; //2013��
		
		//posС��-ÿ��
		String preUserNickSmall=""; //С��ÿ�������ǳ�
		int preUserSmallNum=0; //С��ÿ�����ҳɽ���Ŀ
		double preUserSamllGMV = 0; //С��ÿ�����ҳɽ����
		int smallCount=0;	//С���ɽ���Ŀ
		double smallFee=0; //С���ɽ����
		
		//pos��-ÿ��
		String preUserNickBig=""; //��ÿ�������ǳ�
		int preUserBigNum=0; //��ÿ�����ҳɽ���Ŀ
		double preUserBigGMV=0; //��ÿ�����ҳɽ����
		int bigCount=0;	//�󶨳ɽ���Ŀ
		double bigFee=0; //�󶨳ɽ����
		
		//posС��-2012ÿ��(�ɽ������ͳɽ����)
		int smallOrder2012_count=0;
		double smallOrder2012_fee=0;
		
		//pos��-2012ÿ��(�ɽ������ͳɽ����)
		int bigOrder2012_count=0;
		double bigOrder2012_fee=0;
		
		//posС��-2013ÿ��(�ɽ������ͳɽ����)
		int smallOrder2013_count=0;
		double smallOrder2013_fee=0;
		
		//pos��-2013ÿ��(�ɽ������ͳɽ����)
		int bigOrder2013_count=0;
		double bigOrder2013_fee=0;
		if (!new File(input).exists()) {
			System.out.println("File Not Exist !" + input);
			return;
		}
		List<String> lines = Utils.readWithCharset(input, "utf-8");
		
		//�����³�����С���ɽ����а�(ÿ��)
		Table smallOrderPerdayTable = report.newViewTable(smallOrder_Perday, mainName+"����С���ɽ����а�");
		smallOrderPerdayTable.addCol("�����ǳ�").addCol("��Ʒid").addCol("�ɽ�����").addCol("֧�����ɽ����").addCol(Report.BREAK_VALUE);;
		//�����³�����POS�󶨳ɽ����а�(ÿ��)
		Table bigOrderPerdayTable = report.newViewTable(bigOrder_Perday, mainName+"����POS�󶨳ɽ����а�");
		bigOrderPerdayTable.addCol("�����ǳ�").addCol("��Ʒid").addCol("�ɽ�����").addCol("֧�����ɽ����").addCol(Report.BREAK_VALUE);
		//�����³�2012������С���ɽ����а�(ÿ��)
		Table smallOrder2012Table = report.newViewTable(smallOrder2012, mainName+"2012������С���ɽ����а�");
		smallOrder2012Table.addCol("�����ǳ�").addCol("�ɽ�����").addCol("֧�����ɽ����").addCol(Report.BREAK_VALUE);
		//�����³�2012������POS�󶨳ɽ����а�(ÿ��)
		Table bigOrder2012Table = report.newViewTable(bigOrder2012, mainName+"2012������POS�󶨳ɽ����а�");
		bigOrder2012Table.addCol("�����ǳ�").addCol("�ɽ�����").addCol("֧�����ɽ����").addCol(Report.BREAK_VALUE);
		//�����³�2013������С���ɽ����а�(ÿ��)
		Table smallOrder2013Table = report.newViewTable(smallOrder2013, mainName+"2013������С���ɽ����а�");
		smallOrder2013Table.addCol("�����ǳ�").addCol("�ɽ�����").addCol("֧�����ɽ����").addCol(Report.BREAK_VALUE);
		//�����³�2013������POS�󶨳ɽ����а�(ÿ��)
		Table bigOrder2013Table = report.newViewTable(bigOrder2013, mainName+"2013������POS�󶨳ɽ����а�");
		bigOrder2013Table.addCol("�����ǳ�").addCol("�ɽ�����").addCol("֧�����ɽ����").addCol(Report.BREAK_VALUE);
		
		if (lines!= null && lines.size() > 0) {
			for(String line:lines){
				String[] _cols=StringUtils.split(line,CTRL_A);
				if((_cols!=null&&_cols.length>2)&&(_cols[1].contains("����")||_cols[1].contains("����"))){
					continue;
				}
				if(line.startsWith(smallOrder_Perday)){ //posС��-ÿ��(�ɽ������ͳɽ����)
					if(_cols.length > 0){
						if(StringUtils.isEmpty(preUserNickSmall)||StringUtils.equals(preUserNickSmall,_cols[1])){
							preUserSmallNum=preUserSmallNum+Integer.valueOf(_cols[3]);
							preUserSamllGMV=preUserSamllGMV+Double.parseDouble(_cols[4]);
						}else{
							smallOrderPerdayTable.addCol(preUserNickSmall+"С���ϼ�");
							smallOrderPerdayTable.addCol(preUserNickSmall+"�󶨺ϼ�ItemId","");
							smallOrderPerdayTable.addCol(preUserNickSmall+"�󶨺ϼ���Ŀ",preUserSmallNum+"");
							smallOrderPerdayTable.addCol(preUserNickSmall+"�󶨺ϼ�GMV",preUserSamllGMV+"");
							smallOrderPerdayTable.breakRow();
							preUserSmallNum=0;
							preUserSamllGMV=0;
							preUserSmallNum=preUserSmallNum+Integer.valueOf(_cols[3]);
							preUserSamllGMV=preUserSamllGMV+Double.parseDouble(_cols[4]);
						}
						
						smallOrderPerdayTable.addCol(_cols[1]);
						smallOrderPerdayTable.addCol(_cols[1] + "ITEMID", _cols[2]);
						smallOrderPerdayTable.addCol(_cols[1] + "NUM", _cols[3]);
						smallOrderPerdayTable.addCol(_cols[1] + "GMV", _cols[4]);
						smallOrderPerdayTable.breakRow();
						preUserNickSmall=_cols[1];
						smallCount=smallCount+Integer.valueOf(_cols[3]);
						smallFee=smallFee+Double.parseDouble(_cols[4]);
					}
				}else if(line.startsWith(bigOrder_Perday)){ //pos��-ÿ��(�ɽ������ͳɽ����)
					if(_cols.length > 0){
						if(StringUtils.isEmpty(preUserNickBig)||StringUtils.equals(preUserNickBig,_cols[1])){
							preUserBigNum=preUserBigNum+Integer.valueOf(_cols[3]);
							preUserBigGMV=preUserBigGMV+Double.parseDouble(_cols[4]);
						}else{
							bigOrderPerdayTable.addCol(preUserNickBig+"�󶨺ϼ�");
							bigOrderPerdayTable.addCol(preUserNickBig+"�󶨺ϼ�ItemId","");
							bigOrderPerdayTable.addCol(preUserNickBig+"�󶨺ϼ���Ŀ",preUserBigNum+"");
							bigOrderPerdayTable.addCol(preUserNickBig+"�󶨺ϼ�GMV",preUserBigGMV+"");
							bigOrderPerdayTable.breakRow();
							preUserBigNum=0;
							preUserBigGMV=0;
							preUserBigNum=preUserBigNum+Integer.valueOf(_cols[3]);
							preUserBigGMV=preUserBigGMV+Double.parseDouble(_cols[4]);
						}
						
						bigOrderPerdayTable.addCol(_cols[1]);
						bigOrderPerdayTable.addCol(_cols[1] + "ITEMID", _cols[2]);
						bigOrderPerdayTable.addCol(_cols[1] + "NUM", _cols[3]);
						bigOrderPerdayTable.addCol(_cols[1] + "GMV", _cols[4]);
						bigOrderPerdayTable.breakRow();
						preUserNickBig=_cols[1];
						bigCount=bigCount+Integer.valueOf(_cols[3]);
						bigFee=bigFee+Double.parseDouble(_cols[4]);
					}
					
				}else if(line.startsWith(smallOrder2012)){ //posС��-ÿ��
					smallOrder2012Table.addCol(_cols[1]);
					smallOrder2012Table.addCol(_cols[1] + "NUM", _cols[2]);
					smallOrder2012Table.addCol(_cols[1] + "GMV", _cols[3]);
					smallOrder2012Table.breakRow();
					smallOrder2012_count=smallOrder2012_count+Integer.valueOf(_cols[2]);
					smallOrder2012_fee=smallOrder2012_fee+Double.parseDouble(_cols[3]);
				}else if(line.startsWith(bigOrder2012)){ ////pos��-ÿ��
					bigOrder2012Table.addCol(_cols[1]);
					bigOrder2012Table.addCol(_cols[1] + "NUM", _cols[2]);
					bigOrder2012Table.addCol(_cols[1] + "GMV", _cols[3]);
					bigOrder2012Table.breakRow();
					bigOrder2012_count=bigOrder2012_count+Integer.valueOf(_cols[2]);
					bigOrder2012_fee=bigOrder2012_fee+Double.parseDouble(_cols[3]);
				}else if(line.startsWith(smallOrder2013)){ //posС��-ÿ��
					smallOrder2013Table.addCol(_cols[1]);
					smallOrder2013Table.addCol(_cols[1] + "NUM", _cols[2]);
					smallOrder2013Table.addCol(_cols[1] + "GMV", _cols[3]);
					smallOrder2013Table.breakRow();
					smallOrder2013_count=smallOrder2013_count+Integer.valueOf(_cols[2]);
					smallOrder2013_fee=smallOrder2013_fee+Double.parseDouble(_cols[3]);
				}else if(line.startsWith(bigOrder2013)){ //pos��-ÿ��
					bigOrder2013Table.addCol(_cols[1]);
					bigOrder2013Table.addCol(_cols[1] + "NUM", _cols[2]);
					bigOrder2013Table.addCol(_cols[1] + "GMV", _cols[3]);
					bigOrder2013Table.breakRow();
					bigOrder2013_count=bigOrder2013_count+Integer.valueOf(_cols[2]);
					bigOrder2013_fee=bigOrder2013_fee+Double.parseDouble(_cols[3]);
				}
			}
			
			//posС��-ÿ��
			smallOrderPerdayTable.addCol(preUserNickSmall+"С���ϼ�");
			smallOrderPerdayTable.addCol(preUserNickSmall+"С���ϼ�itemid","");
			smallOrderPerdayTable.addCol(preUserNickSmall+"С���ϼ�����",preUserSmallNum+"");
			smallOrderPerdayTable.addCol(preUserNickSmall+"С���ϼ�GMV",preUserSamllGMV+"");
			smallOrderPerdayTable.breakRow();
			
			smallOrderPerdayTable.addCol("С���ɽ��ϼ�");
			smallOrderPerdayTable.addCol("");
			smallOrderPerdayTable.addCol("С���ɽ��ϼ�NUM",String.valueOf(smallCount));
			smallOrderPerdayTable.addCol("С���ɽ��ϼ�GMV",String.valueOf(smallFee));
			smallOrderPerdayTable.breakRow();
			
			//pos��-ÿ��
			bigOrderPerdayTable.addCol(preUserNickBig+"�󶨺ϼ�");
			bigOrderPerdayTable.addCol(preUserNickBig+"�󶨺ϼ�itemid","");
			bigOrderPerdayTable.addCol(preUserNickBig+"�󶨺ϼ�����",preUserBigNum+"");
			bigOrderPerdayTable.addCol(preUserNickBig+"�󶨺ϼ�GMV",preUserBigGMV+"");
			bigOrderPerdayTable.breakRow();
			
			bigOrderPerdayTable.addCol("�󶨳ɽ��ϼ�");
			bigOrderPerdayTable.addCol("");
			bigOrderPerdayTable.addCol("�󶨳ɽ��ϼ�NUM",String.valueOf(bigCount));
			bigOrderPerdayTable.addCol("�󶨳ɽ��ϼ�GMV",String.valueOf(bigFee));
			bigOrderPerdayTable.breakRow();
			
			//posС��-2012ÿ��
			smallOrder2012Table.addCol("2012С���ɽ��ϼ�");
			smallOrder2012Table.addCol("2012С���ɽ��ϼ�NUM",String.valueOf(smallOrder2012_count));
			smallOrder2012Table.addCol("2012С���ɽ��ϼ�GMV",String.valueOf(smallOrder2012_fee));
			smallOrder2012Table.breakRow();
			
			//pos��-2012ÿ��
			bigOrder2012Table.addCol("2012�󶨳ɽ��ϼ�");
			bigOrder2012Table.addCol("2012�󶨳ɽ��ϼ�NUM",String.valueOf(bigOrder2012_count));
			bigOrder2012Table.addCol("2012�󶨳ɽ��ϼ�GMV",String.valueOf(bigOrder2012_fee));
			bigOrder2012Table.breakRow();
			
			//posС��-2013ÿ��
			smallOrder2013Table.addCol("2013С���ɽ��ϼ�");
			smallOrder2013Table.addCol("2013С���ɽ��ϼ�NUM",String.valueOf(smallOrder2013_count));
			smallOrder2013Table.addCol("2013С���ɽ��ϼ�GMV",String.valueOf(smallOrder2013_fee));
			smallOrder2013Table.breakRow();
			
			//pos��-2013ÿ��
			bigOrder2013Table.addCol("2013�󶨳ɽ��ϼ�");
			bigOrder2013Table.addCol("2013�󶨳ɽ��ϼ�NUM",String.valueOf(bigOrder2013_count));
			bigOrder2013Table.addCol("2013�󶨳ɽ��ϼ�GMV",String.valueOf(bigOrder2013_fee));
			bigOrder2013Table.breakRow();
		} 
		
		XmlReportFactory.dump(report, new FileOutputStream(input + ".xml"));
	}
}
