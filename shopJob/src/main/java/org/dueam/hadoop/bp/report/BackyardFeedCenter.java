package org.dueam.hadoop.bp.report;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.math.NumberUtils;
import org.dueam.hadoop.common.util.MapUtils;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.hadoop.services.Category;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

public class BackyardFeedCenter {

	public static void main(String[] args) throws IOException {
		Report report = Report.newReport("feed统计");
		String input = args[0];
        if (!new File(input).exists()) {
            System.out.println("File Not Exist ! => " + input);
            return;
        }
        Map<String, List<String[]>> today = MapUtils.map(Utils.read(input, (String[]) null));
        if (today == null) {
            System.out.println("No Data ! => " + input);
            return;
        }
        
        if (true && today.containsKey("favor")) {
        	 Map<String, Long> _countMap = new HashMap<String, Long>();
        	 for (String[] array : today.get("favor")) {
        		 String userNick = array[0];
        		 long value = NumberUtils.toLong(array[1]);
        		 Long _v = _countMap.get(userNick);
	                if (_v == null) {
	                    _v = 0L;
	                }
	                _v = _v + value;
	                double pm=_v/1000.0;
	                _countMap.put(userNick, (long) pm);
        	 }
        	 Table table = report.newGroupTable("favor", "活动feed数据 - 喜欢次数");
        	 for (Map.Entry<String, Long> entry : _countMap.entrySet()) {
	                table.addCol(entry.getKey(), Category.getCategoryName(entry.getKey()),String.valueOf(entry.getValue()));
	         }
	         table.sort(Table.SORT_VALUE);
        }
	}
}
