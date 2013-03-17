package org.dueam.hadoop.bp.report;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.dueam.hadoop.common.tables.BmwShops;
import org.dueam.hadoop.common.util.MapUtils;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.hadoop.services.Category;
import org.dueam.hadoop.services.CategoryDO;
import org.dueam.hadoop.utils.uic.UicUtils;
import org.dueam.hadoop.utils.uic.UserDO;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class MallOnlineItemList {


    /**
     * @param args
     * @throws java.io.IOException
     */
    public static void main(String[] args) throws IOException {
        Report report = Report.newReport("商城商品被导航情况分析");
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
        for (String catType : new String[]{"1", "2"}) {
            if (true && today.containsKey(catType)) {

                Map<String, List<String[]>> statusMap = MapUtils.map(today.get(catType));

                if (true) {
                    Table table = report.newTable("top_cat_" + catType, "商城商品无法被第" + catType + "套前台类目导航的后台类目TOP");
                    List<String[]> list = statusMap.get("TOP_CATEGORY");
                    Collections.sort(list, new Comparator<String[]>() {
                        public int compare(String[] o1, String[] o2) {
                            return NumberUtils.toLong(o1[1]) < NumberUtils.toLong(o2[1]) ? 1 : -1;
                        }
                    });
                    int count = 0;
                    for (String[] array : list) {
                        if (null != array && array.length > 1) {
                            CategoryDO category = Category.getCategory(array[0]);
                            if (category != null) {
                                table.addCol(array[0], category.getName() + "[" + category.getCatPath() + "]", array[1]);
                            }
                        }
                        if (count++ > 50) break;
                    }
                }
                if (true) {

                    Table table = report.newTable("top_seller_" + catType, "商城商品无法被第" + catType + "套前台类目导航的卖家类目TOP");
                    List<String[]> list = statusMap.get("TOP_SELLER");
                    Collections.sort(list, new Comparator<String[]>() {
                        public int compare(String[] o1, String[] o2) {
                            return NumberUtils.toLong(o1[1]) < NumberUtils.toLong(o2[1]) ? 1 : -1;
                        }
                    });
                    int count = 0;
                    for (String[] array : list) {
                        if (null != array && array.length > 1) {
                            UserDO user = UicUtils.getUser(array[0]);
                            if (!user.isCC()) {
                                table.addCol(array[0], user.getNick(), array[1]);
                                if (count++ > 50) break;
                            }
                        }

                    }
                }
            }
        }


        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
    }
}
