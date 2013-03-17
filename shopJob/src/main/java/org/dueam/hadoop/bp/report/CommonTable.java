package org.dueam.hadoop.bp.report;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.dueam.hadoop.common.util.Fmt;
import org.dueam.hadoop.common.util.MapUtils;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.dueam.hadoop.common.tables.Cart.status;

/**
 * User: windonly
 * Date: 11-1-6 下午5:24
 */
public class CommonTable {
    private static Map<String, String> todayOrder = Utils.asLinkedMap("last", "今天最后一次加入购物车并下单", "created", "今天第一次加入购物车并下单");

    public static class Col {
        public Col(String key, String title, boolean show) {
            this.key = key;
            this.title = title;
            this.show = show;
        }

        public String key;
        public String title;
        public boolean show = true;

        public static List<Col> getCols(String input) {
            List<Col> cols = new ArrayList<Col>();
            for (String line : StringUtils.split(input, ',')) {
                boolean show = !StringUtils.startsWith(line, "^");
                line = StringUtils.removeStart(line, "^");
                line = StringUtils.replaceChars(line, ' ', '$');
                String[] array = StringUtils.split(line, '=');
                if (array.length > 1) {
                    cols.add(new Col(array[0], array[1], show));
                } else {
                    cols.add(new Col(array[0], array[0], show));
                }
            }
            return cols;
        }

    }
    public static char CTRL_A = (char)0x01;
    public static void main(String[] args) throws IOException {
        if (args.length < 3) {
            System.out.print("args : input title cols");
            return;
        }

        Report report = Report.newReport(args[1]);
        String input = args[0];
        if (!new File(input).exists()) {
            return;
        }
        List<Col> colList = Col.getCols(args[2]);
        Table commonTable = report.newViewTable("common","基本信息汇总表");
        for(Col col : colList){
            if(col.show)
            commonTable.addCol(col.title);
        }
        commonTable.breakRow();
        for(String line : Utils.readWithCharset(input,"utf-8")){
            String[] cols = StringUtils.splitPreserveAllTokens(line,CTRL_A);
            int pos = 0;
            String prefix = cols[0];
            Table table = report.newTable("detail"+"_"+prefix,prefix + "详情表");
            boolean isFirst = true;
            for(Col col : colList){
                if(col.show){
                    if(isFirst){
                        commonTable.addCol(cols[pos]);
                        isFirst = false;
                    }else   {
                        commonTable.addCol(prefix+"_"+col.key,col.title,Fmt.fixed(cols[pos]));
                    }

                }
                if(pos > 0){
                    table.addCol(col.key,col.title,Fmt.fixed(cols[pos]));
                }
                pos++;
            }
            commonTable.breakRow();
        }

        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));

    }
}
