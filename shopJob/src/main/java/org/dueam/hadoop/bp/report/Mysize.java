package org.dueam.hadoop.bp.report;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.dueam.hadoop.common.util.Fmt;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

/**
 * 尺码库相关数据统计
 */
public class Mysize {
    private static char TAB = (char) 0x09;
    static Map<String, String> typeMap = Utils.asLinkedMap("jac", "脚长", "jak", "脚宽", "sxw","上胸围","xxw", "下胸围", "xw",
    		"胸围", "yw","腰围","tw", "臀围","bc","臂长","jk", "肩宽", "tc", "腿长","sg","身高","tz","体重","dtw","大腿围");

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

        Report report = Report.newReport("尺码库相关数据统计");

        Table role = report.newGroupTable("roleNumber", "用户所建角色个数统计");
       // role.addCol("所建角色数").addCol("用户数").addCol("总用户数").addCol("所占比率").addCol(Report.BREAK_VALUE);

        Table hot = report.newGroupTable("hotData", "热点部位数据统计");
       // hot.addCol("部位名称").addCol("用户数").addCol("总用户数").addCol("所占比率").addCol(Report.BREAK_VALUE);

        Table foot = report.newViewTable("footLength", "脚长数据参考占比统计");
        foot.addCol("用户数").addCol("总用户数").addCol("所占比率").addCol(Report.BREAK_VALUE);

        Table sex = report.newViewTable("sex", "角色性别分布");
        sex.addCol("妹妹").addCol("总用户数").addCol("所占比率").addCol(Report.BREAK_VALUE);

        Table quantity = report.newViewTable("quantity", "净占比统计，统计创建档案的用户真正填写数据情况");
        quantity.addCol("净填写用户数").addCol("总用户数").addCol("所占比率").addCol(Report.BREAK_VALUE);

        for (String line : Utils.readWithCharset(input, "utf-8")) {
        	if(line == null) return;
            String[] _allCols = StringUtils.splitByWholeSeparator(line, "\t");

            String type = _allCols[0];

            if ("part".equalsIgnoreCase(type)) {

                String jac = _allCols[1];
                String jak = _allCols[2];
                String sxw = _allCols[3];
                String xxw = _allCols[4];
                String xw = _allCols[5];
                String yw = _allCols[6];
                String tw = _allCols[7];
                String bc = _allCols[8];
                String jk = _allCols[9];
                String tc = _allCols[10];
                String sg = _allCols[11];
                String tz = _allCols[12];
                String dtw = _allCols[13];

            	hot.addCol(typeMap.get("jac"),jac);
            	hot.addCol(typeMap.get("jak"),jak);
            	hot.addCol(typeMap.get("sxw"),sxw);
            	hot.addCol(typeMap.get("xxw"),xxw);
            	hot.addCol(typeMap.get("jk"),jk);
            	hot.addCol(typeMap.get("xw"),xw);
            	hot.addCol(typeMap.get("yw"),yw);
            	hot.addCol(typeMap.get("tw"),tw);
            	hot.addCol(typeMap.get("dtw"),dtw);
            	hot.addCol(typeMap.get("bc"),bc);
            	hot.addCol(typeMap.get("sg"),sg);
            	hot.addCol(typeMap.get("tz"),tz);
            	hot.addCol(typeMap.get("tc"),tc);

            }
            if("1".equalsIgnoreCase(type)||"2".equalsIgnoreCase(type)||"3".equalsIgnoreCase(type)||"4".equalsIgnoreCase(type)||"5".equalsIgnoreCase(type)){
            	role.addCol(type,_allCols[1]);  //创建了1个角色的用户数
            }
            if("quanlity".equalsIgnoreCase(type)){
            	quantity.addCol(_allCols[1]).addCol(_allCols[2]).addCol(Fmt.parent2(_allCols[1], _allCols[2])).breakRow();  //此字段代表创建角色后确实填写了数据的用户数
            }
            if("foot".equalsIgnoreCase(type)){
            	foot.addCol(_allCols[1]).addCol(_allCols[2]).addCol(Fmt.parent2(_allCols[1], _allCols[2])).breakRow();  //此字段代表参考参照表的用户数
            }
            if("sex".equalsIgnoreCase(type)){
            	sex.addCol(_allCols[1]).addCol(_allCols[2]).addCol(Fmt.parent2(_allCols[1], _allCols[2])).breakRow();  //此字段代表参考参照表的用户数
            }
        }
        role.sort(Table.SORT_KEY);
        hot.sort(Table.SORT_VALUE);
        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
    }

}
