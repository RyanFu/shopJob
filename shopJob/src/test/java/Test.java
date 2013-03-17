import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.fs.FileUtil;
import org.dueam.hadoop.common.Area;
import org.dueam.hadoop.common.KeyValuePair;
import org.dueam.hadoop.common.tables.TcBizOrder;
import org.dueam.hadoop.common.util.DateStringUtils;
import org.dueam.hadoop.common.util.Fmt;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.hadoop.jobs.AuctionPrice;
import org.dueam.hadoop.services.Category;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.dueam.hadoop.common.Functions.newArrayList;

/**
 * User: windonly
 * Date: 11-1-7 上午11:00
 */
public class Test {
    public static void main(String[] args) throws Exception {
        if(true){
            System.out.println(Fmt.fixed("123456789"));
            System.out.println(Fmt.fixed("1234567.89"));
            System.out.println(Fmt.fixed("12345.6789"));
            System.out.println(Fmt.fixed("123.456789"));
        }
        if (false) {
            List<KeyValuePair> list = newArrayList();
            list.add(KeyValuePair.newPair("2011-01-01", 8550, 11, KeyValuePair.DateKey));
            list.add(KeyValuePair.newPair("2011-01-02", 9500, 12, KeyValuePair.DateKey));
            list.add(KeyValuePair.newPair("2011-01-03", 9500, 13, KeyValuePair.DateKey));
            list.add(KeyValuePair.newPair("2011-01-04", 9500, 14, KeyValuePair.DateKey));
            list.add(KeyValuePair.newPair("2011-01-05", 8550, 15, KeyValuePair.DateKey));
            StringBuffer sb = new StringBuffer();
            AuctionPrice.doFilter(list,sb);
            System.out.print(sb);
            return;
        }

        System.out.println(DateStringUtils.add("20090114", 1313));
        String input = "ABCzabz123,--，您好。";
        for (byte b : input.getBytes()) {
            System.out.print((int) b);
            System.out.print((char) 0x09);
        }

        System.exit(1);
        System.out
                .println("2011-05-10 23:34:11".compareTo("2011-03-10 00:00:00"));


        String time = StringUtils.substring("20110531003700", 8, 12);
        time = time.substring(0, 2) + ":" + time.substring(2);
        System.out.println(time);
        System.exit(1);


        List<String> list = FileUtils.readLines(new File("target/buyer_come_back"), "GBK");
        Map<String, Long> countMap = new HashMap<String, Long>();
        Map<String, Long> totalMap = new HashMap<String, Long>();
        for (String line : list) {
            String[] _allCols = StringUtils.splitPreserveAllTokens(line, (char) 0x09);
            Long sum = totalMap.get(_allCols[1]);
            if (sum == null) {
                sum = 0L;
            }
            sum = sum + NumberUtils.toLong(_allCols[2]);
            totalMap.put(_allCols[1], sum);
            countMap.put(_allCols[1] + "\t" + _allCols[0], NumberUtils.toLong(_allCols[2]));
        }
        final Area area = Area.newArea(1, 2, 3, 4, 5, 10, 20, 50, 100, 200);
        System.out.print("类目ID,类目名称,总数");
        for (String key : area.areaKeys) {
            System.out.print("," + key + ",区间占比");
        }
        System.out.println("");

        for (String catId : totalMap.keySet()) {
            if ("NULL".equals(catId)) continue;
            System.out.print(catId + "," + Category.getCategoryName(catId) + "," + totalMap.get(catId));
            for (String key : area.areaKeys) {
                Long value = countMap.get(catId + "\t" + key);
                if (value == null) value = 0L;
                System.out.print("," + value + "," + Fmt.parent(value, totalMap.get(catId)));
            }
            System.out.println("");
        }


    }
}
