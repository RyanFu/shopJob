import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.dueam.hadoop.common.util.Fmt;
import org.dueam.hadoop.services.Category;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * User: windonly
 * Date: 11-5-23 下午5:40
 */
public class SpuMarket {
    public static void main(String[] args) throws IOException {
        Map<String, String> online_cat_order_count = toMap("online_cat_order_count");
        Map<String, String> online_item_count = toMap("online_item_count");
        Map<String, String> online_item_spu_count = toMap("online_item_spu_count");
        Map<String, String> online_spu_count = toMap("online_spu_count");
        Map<String, String> online_spu_order_count = toMap("online_spu_order_count");
        Map<String, String> online_item_pv_count = toMap("online_item_pv_count");
        Map<String, String> online_spu_pv_count = toMap("online_spu_pv_count");
        Map<String, String> online_spu_has_order_count = toMap("online_spu_has_order_count");
        Map<String, String> online_spu_has_order_day_count = toMap("online_spu_has_order_day_count");

        List<String> output = new ArrayList<String>();
        String line = StringUtils.join(new String[]{
                "类目ID",
                "类目名称",
                "在线商品数量",
                "绑定小二确认Spu商品数",
                "占比",
                "绑定商家确认Spu商品数",
                "占比",
                "绑定未确认Spu商品数",
                "占比",
                "过去50天产生交易的订单数", "绑定小二确认Spu订单数"
                , "占比", "绑定商家确认Spu订单数", "占比", "绑定未确认Spu订单数", "占比",
                "全网Spu总数", "全网小二确认Spu数", "占比", "全网商家确认Spu数", "占比", "全网未确认Spu数", "占比",
                "过去50天产生交易的小二确认Spu数","占比","过去50天产生交易的商家确认Spu数","占比","过去50天产生交易的未确认Spu数","占比",
                "5月19日产生交易的小二确认Spu数","占比","5月19日产生交易的商家确认Spu数","占比","5月19日产生交易的未确认Spu数","占比",
                "5月22日商品PV","绑定小二确认Spu商品PV",

                "占比",
                "绑定商家确认Spu商品PV",
                "占比",
                "绑定未确认Spu商品PV",
                "占比"}, ",");
        output.add(line);
        for (String catId : online_item_count.keySet()) {
            long sumSpuCount = NumberUtils.toLong(online_spu_count.get(catId + A + '3')) + NumberUtils.toLong(online_spu_count.get(catId + A + '0')) + NumberUtils.toLong(online_spu_count.get(catId + A + '2')) + NumberUtils.toLong(online_spu_count.get(catId + A + '1')) ;
            line = StringUtils.join(new String[]{
                    catId,
                    Category.getCategoryName(catId),
                    online_item_count.get(catId),
                    online_item_spu_count.get(catId + A + '3'),
                    Fmt.parent(NumberUtils.toLong(online_item_spu_count.get(catId + A + '3')), NumberUtils.toLong(online_item_count.get(catId))),
                    online_item_spu_count.get(catId + A + '0'),
                    Fmt.parent(NumberUtils.toLong(online_item_spu_count.get(catId + A + '0')), NumberUtils.toLong(online_item_count.get(catId))),
                    online_item_spu_count.get(catId + A + '2'),
                    Fmt.parent(NumberUtils.toLong(online_item_spu_count.get(catId + A + '2')), NumberUtils.toLong(online_item_count.get(catId))),
                    online_cat_order_count.get(catId),
                    online_spu_order_count.get(catId + A + '3'),
                    Fmt.parent(NumberUtils.toLong(online_spu_order_count.get(catId + A + '3')), NumberUtils.toLong(online_cat_order_count.get(catId))),
                    online_spu_order_count.get(catId + A + '0'),
                    Fmt.parent(NumberUtils.toLong(online_spu_order_count.get(catId + A + '0')), NumberUtils.toLong(online_cat_order_count.get(catId))),
                    online_spu_order_count.get(catId + A + '2'),
                    Fmt.parent(NumberUtils.toLong(online_spu_order_count.get(catId + A + '2')), NumberUtils.toLong(online_cat_order_count.get(catId))),
                    String.valueOf(sumSpuCount),
                    online_spu_count.get(catId + A + '3'),
                    Fmt.parent(NumberUtils.toLong(online_spu_count.get(catId + A + '3')), sumSpuCount),
                    online_spu_count.get(catId + A + '0'),
                    Fmt.parent(NumberUtils.toLong(online_spu_count.get(catId + A + '0')), sumSpuCount),
                    online_spu_count.get(catId + A + '2'),
                    Fmt.parent(NumberUtils.toLong(online_spu_count.get(catId + A + '2')), sumSpuCount),
                    //过去50天有交易的Spu数量
                    online_spu_has_order_count.get(catId + A + '3'),
                    Fmt.parent(NumberUtils.toLong(online_spu_has_order_count.get(catId + A + '3')), sumSpuCount),
                    online_spu_has_order_count.get(catId + A + '0'),
                    Fmt.parent(NumberUtils.toLong(online_spu_has_order_count.get(catId + A + '0')), sumSpuCount),
                    online_spu_has_order_count.get(catId + A + '2'),
                    Fmt.parent(NumberUtils.toLong(online_spu_has_order_count.get(catId + A + '2')), sumSpuCount),

                    //过去50天有交易的Spu数量
                    online_spu_has_order_day_count.get(catId + A + '3'),
                    Fmt.parent(NumberUtils.toLong(online_spu_has_order_day_count.get(catId + A + '3')), sumSpuCount),
                    online_spu_has_order_day_count.get(catId + A + '0'),
                    Fmt.parent(NumberUtils.toLong(online_spu_has_order_day_count.get(catId + A + '0')), sumSpuCount),
                    online_spu_has_order_day_count.get(catId + A + '2'),
                    Fmt.parent(NumberUtils.toLong(online_spu_has_order_day_count.get(catId + A + '2')), sumSpuCount),

                    online_item_pv_count.get(catId),
                    online_spu_pv_count.get(catId + A + '3'),
                    Fmt.parent(NumberUtils.toDouble(online_spu_pv_count.get(catId + A + '3')), NumberUtils.toDouble(online_item_pv_count.get(catId))),
                    online_spu_pv_count.get(catId + A + '0'),
                    Fmt.parent(NumberUtils.toDouble(online_spu_pv_count.get(catId + A + '0')), NumberUtils.toDouble(online_item_pv_count.get(catId))),
                    online_spu_pv_count.get(catId + A + '2'),
                    Fmt.parent(NumberUtils.toDouble(online_spu_pv_count.get(catId + A + '2')), NumberUtils.toDouble(online_item_pv_count.get(catId)))
            }, ",");
            output.add(line);
        }
        FileUtils.writeLines(new File("output.csv"),"GBK",output);
    }

    final static char A = 0x01;

    public static Map<String, String> toMap(String input) {
        input = "C:\\Users\\Administrator\\Desktop\\spu_market\\" + input + "\\" + input;
        try {
            List<String> list = FileUtils.readLines(new File(input));
            Map<String, String> map = new HashMap<String, String>();
            for (String line : list) {
                int pos = line.lastIndexOf(A);
                if (pos > 0) {
                    map.put(line.substring(0, pos), line.substring(pos + 1));
                }
            }
            return map;
        } catch (IOException e) {
            //
            e.printStackTrace();
        }
        return null;
    }
}
