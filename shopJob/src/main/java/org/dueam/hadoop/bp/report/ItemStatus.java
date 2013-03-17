package org.dueam.hadoop.bp.report;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.dueam.hadoop.common.util.ItemUtils;
import org.dueam.hadoop.utils.TaobaoPath;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ItemStatus{

	/**
	 * @param args
	 * @throws java.io.IOException
	 */
	public static void main(String[] args) throws IOException {
        Report report = Report.newReport("全网商品分布以及增长趋势追踪");
        String input = args[0];
        if (!new File(input).exists()) {
            System.out.println("File Not Exist ! => " + input);
            return;
        }
        Map<String, String> statusMap = getMap( input);
        if(statusMap == null){
            System.out.println("No Data ! => " + input);
            return;
        }
        if(true){
            Table table = report.newTable("total","全网商品总览");
            table.setType("main");
            table.addCol("total_all","全网商品",sum(statusMap, "ALL"));
            table.addCol("total_today","今天发布商品",sum(statusMap, TaobaoPath.dateFormat(input)));
        }
        if(true){
            Table table = report.newGroupTable("status_all", "全网商品的各个状态分布");
            for(String status : ItemUtils.allStatus) {
                    String value = statusMap.get("ALL" + "^" + status);
                    table.addCol(status, ItemUtils.getItemStatusName(status),value);
            }
        }

        if(true){
            Table table = report.newGroupTable("status_today", "今天发布的商品各个状态分布");
            for(String status : ItemUtils.allStatus) {
                    String value = statusMap.get(TaobaoPath.dateFormat(input) + "^" + status);
                    table.addCol(status, ItemUtils.getItemStatusName(status),value);
            }
        }
        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
	}

    static String sum(Map<String, String> statusMap, String key) {
        long sum = 0;
        for (String status : ItemUtils.allStatus) {
            String newKey = key + "^" + status;
            sum += NumberUtils.toLong(statusMap.get(newKey) + "", 0);

        }
        if (sum <= 0) return "0";
        return String.valueOf(sum);
    }

	@SuppressWarnings("unchecked")
	static Map<String, String> getMap( String date) {
		try {
			Map<String, String> statusMap = new HashMap<String, String>();
			for (String line : (List<String>) FileUtils.readLines(new File(date), "GBK")) {
				String[] _array = StringUtils.split(line, "\t");
				String gmtDate = _array[0];
				String status = _array[1];
				String count = _array[2];
				statusMap.put(gmtDate + "^" + status, count);
			}
			return statusMap;
		} catch (Exception e) {
		}
		return null;
	}
}
