package org.dueam.hadoop.bp.report;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

/**
 * 新版detail页面头部引导效果
 * 
 * @author linyi
 */
public class DetailKeywordsReport {
    private static char CTRL_A = (char) 0x01;

    private static Map<String, String> catMap = new HashMap<String, String>();

    static {

        catMap.put("spm=2013.1.1.1", "服饰");
        catMap.put("spm=2013.1.1.2", "女装");
        catMap.put("spm=2013.1.1.3", "男装");
        catMap.put("spm=2013.1.1.4", "运动");
        catMap.put("spm=2013.1.1.5", "女鞋");
        catMap.put("spm=2013.1.1.6", "箱包");
        catMap.put("spm=2013.1.2.1", "数码");
        catMap.put("spm=2013.1.2.2", "手机");
        catMap.put("spm=2013.1.2.4", "相机");
        catMap.put("spm=2013.1.2.5", "家电");
        catMap.put("spm=2013.1.2.6", "配件");
        catMap.put("spm=2013.1.2.3", "笔记本");
        catMap.put("spm=2013.1.3.1", "生活");
        catMap.put("spm=2013.1.3.2", "家居");
        catMap.put("spm=2013.1.3.4", "母婴");
        catMap.put("spm=2013.1.3.5", "食品");
        catMap.put("spm=2013.1.4.6", "汽车");
        catMap.put("spm=2013.1.3.6", "婚庆");
        catMap.put("spm=2013.1.4.1", "护肤");
        catMap.put("spm=2013.1.4.2", "手表");
        catMap.put("spm=2013.1.4.3", "饰品");
        catMap.put("spm=2013.1.4.4", "眼镜");
        catMap.put("spm=2013.1.4.5", "花鸟");
        catMap.put("spm=2013.1.3.3", "百货");

    }

    /**
     * @param args
     * @throws java.io.IOException
     */
    public static void main(String[] args) throws IOException {
        String input = args[0];
        if (!new File(input).exists()) {
            System.out.println("File Not Exist ! => " + input);
            return;
        }

        Report report = Report.newReport("新版detail页面头部引导报表");

        Map<String, Table> summayMap = new HashMap<String, Table>();

        for (Entry<String, String> entry : catMap.entrySet()) {
            Table summay = report.newTable(entry.getKey(), entry.getValue());
            summayMap.put(entry.getKey(), summay);
        }

        for (String line : Utils.readWithCharset(input, "utf-8")) {
            String[] allCols = StringUtils.splitPreserveAllTokens(line, CTRL_A);

            Table summay = summayMap.get(allCols[0]);
            if (summay == null) {
                continue;
            }

            summay.addCol("PV", allCols[1]);
            summay.addCol("UV", allCols[2]);
            summay.addCol("UV(MID)", allCols[3]);

        }

        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
    }
}
