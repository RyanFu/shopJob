import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.io.IOException;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: windonly
 * Date: 11-12-20
 * Time: 下午8:42
 * To change this template use File | Settings | File Templates.
 */
public class ListMarket {
    public static void main(String[] args) throws Exception {
        List<String> input = (List<String>) FileUtils.readLines(new File("/Users/windonly/Dropbox/home/tb/hadoop/input.csv"));
        Map<String,String> markMap = new HashMap<String, String>();
        for(String[] array : mark){
            markMap.put(array[0],array[1]);
        }

        List<String> result = new ArrayList<String>();
        result.add("\"市场名称\","+input.get(0));
        for (int i=1;i<input.size();i++) {
            String line = input.get(i);
            String[] cols = StringUtils.split(line,',');
            String id = StringUtils.remove(cols[1],'"');
            result.add("\""+markMap.get(id)+"\","+line);
        }

        FileUtils.writeLines(new File("/Users/windonly/Dropbox/home/tb/hadoop/pv.csv"),"GBK",result);
    }

    public static String[][] mark = {{"0", "非市场"},
            {"869", "母婴市场"},
            {"874", "女装市场"},
            {"872", "数码市场"},
            {"543", "珠宝市场"},
            {"544", "健康市场"},

            {"546", "运动市场"},
            {"548", "百货市场"},
            {"861", "家居市场"},
            {"843", "美容市场"},
            {"551","饰品市场"},
            {"552", "手表市场"},
            {"895", "男装市场"},
            {"568", "玩具市场"},
            {"569", "汽车用品市场"},
            {"571", "家纺市场"},
            {"898", "内衣市场"},
            {"576", "爱好市场"},
            {"578", "虚拟市场"},
            {"581", "眼镜市场"},
            {"582", "卡券市场"},
            {"808", "房产市场"},
            {"835", "淘宝网厅"},
            {"894", "女包市场"},
            {"897", "男包市场"},
            {"899", "配饰市场"}};
}
