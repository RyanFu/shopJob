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
 * 市场顶部搜索数据统计报表
 */
public class MarketSearchReport {
	 private static char CTRL_A = (char) 0x01;
	    public static void main(String[] args) throws IOException {
	        String input = args[0];
	        if (!new File(input).exists()) {
	            System.out.println("File Not Exist ! => " + input);
	            return;
	        }
	        Report report = Report.newReport("市场顶部搜索的数据统计报表");
	        Table topTable = report.newGroupTable("topTable", "顶部搜索数据汇总");
	        Table marketTable = report.newTable("marketSearch", "顶部市场搜索数据统计");

	        String marketSearch = null;
	        String noresultOfmarketSearch = null;

	        for (String line : Utils.readWithCharset(input, "utf-8")) {
	            String[] _allCols = StringUtils.splitPreserveAllTokens(line, CTRL_A);
	            if (_allCols.length > 1) {
	                if (StringUtils.equals(_allCols[0], "market")) {
	                	marketSearch = _allCols[1];
	                	topTable.addCol("市场搜索的pv", _allCols[1]);
	                	marketTable.addCol("市场顶部搜索的pv", _allCols[1]);
	                	marketTable.addCol("list总的pv", _allCols[2]);
	                	marketTable.addCol("市场顶部搜索占list的比重",Fmt.div(_allCols[1], _allCols[2]));
	                }else if(StringUtils.equals(_allCols[0],"noresult")){
	                	noresultOfmarketSearch = _allCols[1];
	                	marketTable.addCol("市场顶部搜索无结果的pv", _allCols[1]);
	                	marketTable.addCol("list总的无结果pv", _allCols[2]);
	                	marketTable.addCol("市场无结果占list总的无结果的比重",Fmt.div(_allCols[1], _allCols[2]));
	                }else if(StringUtils.equals(_allCols[0], "search")){
	                	topTable.addCol("主搜索的pv", _allCols[1]);
	                }
	            }
	        }
	        marketTable.addCol("市场无结果占市场搜索的比重", Fmt.div(noresultOfmarketSearch,marketSearch));
	        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
	    }
}
