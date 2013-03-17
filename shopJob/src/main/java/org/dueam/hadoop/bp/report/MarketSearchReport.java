package org.dueam.hadoop.bp.report;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.dueam.hadoop.common.util.Fmt;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

/**
 * �г�������������ͳ�Ʊ���
 */
public class MarketSearchReport {
	 private static char CTRL_A = (char) 0x01;
	    public static void main(String[] args) throws IOException {
	        String input = args[0];
	        if (!new File(input).exists()) {
	            System.out.println("File Not Exist ! => " + input);
	            return;
	        }
	        Report report = Report.newReport("�г���������������ͳ�Ʊ���");
	        Table topTable = report.newGroupTable("topTable", "�����������ݻ���");
	        Table marketTable = report.newTable("marketSearch", "�����г���������ͳ��");

	        String marketSearch = null;
	        String noresultOfmarketSearch = null;

	        for (String line : Utils.readWithCharset(input, "utf-8")) {
	            String[] _allCols = StringUtils.splitPreserveAllTokens(line, CTRL_A);
	            if (_allCols.length > 1) {
	                if (StringUtils.equals(_allCols[0], "market")) {
	                	marketSearch = _allCols[1];
	                	topTable.addCol("�г�������pv", _allCols[1]);
	                	marketTable.addCol("�г�����������pv", _allCols[1]);
	                	marketTable.addCol("list�ܵ�pv", _allCols[2]);
	                	marketTable.addCol("�г���������ռlist�ı���",Fmt.div(_allCols[1], _allCols[2]));
	                }else if(StringUtils.equals(_allCols[0],"noresult")){
	                	noresultOfmarketSearch = _allCols[1];
	                	marketTable.addCol("�г����������޽����pv", _allCols[1]);
	                	marketTable.addCol("list�ܵ��޽��pv", _allCols[2]);
	                	marketTable.addCol("�г��޽��ռlist�ܵ��޽���ı���",Fmt.div(_allCols[1], _allCols[2]));
	                }else if(StringUtils.equals(_allCols[0], "search")){
	                	topTable.addCol("��������pv", _allCols[1]);
	                }
	            }
	        }
	        marketTable.addCol("�г��޽��ռ�г������ı���", Fmt.div(noresultOfmarketSearch,marketSearch));
	        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
	    }
}
