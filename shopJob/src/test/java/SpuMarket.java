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
 * Date: 11-5-23 ����5:40
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
                "��ĿID",
                "��Ŀ����",
                "������Ʒ����",
                "��С��ȷ��Spu��Ʒ��",
                "ռ��",
                "���̼�ȷ��Spu��Ʒ��",
                "ռ��",
                "��δȷ��Spu��Ʒ��",
                "ռ��",
                "��ȥ50��������׵Ķ�����", "��С��ȷ��Spu������"
                , "ռ��", "���̼�ȷ��Spu������", "ռ��", "��δȷ��Spu������", "ռ��",
                "ȫ��Spu����", "ȫ��С��ȷ��Spu��", "ռ��", "ȫ���̼�ȷ��Spu��", "ռ��", "ȫ��δȷ��Spu��", "ռ��",
                "��ȥ50��������׵�С��ȷ��Spu��","ռ��","��ȥ50��������׵��̼�ȷ��Spu��","ռ��","��ȥ50��������׵�δȷ��Spu��","ռ��",
                "5��19�ղ������׵�С��ȷ��Spu��","ռ��","5��19�ղ������׵��̼�ȷ��Spu��","ռ��","5��19�ղ������׵�δȷ��Spu��","ռ��",
                "5��22����ƷPV","��С��ȷ��Spu��ƷPV",

                "ռ��",
                "���̼�ȷ��Spu��ƷPV",
                "ռ��",
                "��δȷ��Spu��ƷPV",
                "ռ��"}, ",");
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
                    //��ȥ50���н��׵�Spu����
                    online_spu_has_order_count.get(catId + A + '3'),
                    Fmt.parent(NumberUtils.toLong(online_spu_has_order_count.get(catId + A + '3')), sumSpuCount),
                    online_spu_has_order_count.get(catId + A + '0'),
                    Fmt.parent(NumberUtils.toLong(online_spu_has_order_count.get(catId + A + '0')), sumSpuCount),
                    online_spu_has_order_count.get(catId + A + '2'),
                    Fmt.parent(NumberUtils.toLong(online_spu_has_order_count.get(catId + A + '2')), sumSpuCount),

                    //��ȥ50���н��׵�Spu����
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
