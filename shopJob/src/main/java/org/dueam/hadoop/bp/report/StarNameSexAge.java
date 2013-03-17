package org.dueam.hadoop.bp.report;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.dueam.hadoop.common.Area;
import org.dueam.hadoop.common.tables.RFactSellPV;
import org.dueam.hadoop.common.util.Fmt;
import org.dueam.hadoop.common.util.MapUtils;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * List的用户星级、年龄、性别统计
 */
public class StarNameSexAge {
    private static char CTRL_A = (char) 0x01;
    private static String STAR = "star";
	private static String GENDER = "gender";
	private static String AGE = "age";

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

        Report report = Report.newReport("星级、年龄、性别分布统计");

        Table star = report.newGroupTable("star", "星级分布统计");
        Table gender = report.newGroupTable("gender", "性别分布统计");
        Table age = report.newGroupTable("age", "年龄分布统计");

        Area areaAge = Area.newArea(-1, 1, 25, 30, 40, 50, 60);

        FileInputStream fi = new FileInputStream(input);
        BufferedReader dr = new BufferedReader(new InputStreamReader(fi,"utf-8"));
        String line =  dr.readLine();
        boolean isAge = false;
        while(line!= null){
            String[] _allCols = StringUtils.splitPreserveAllTokens(line, CTRL_A);
            if (StringUtils.equals(STAR,_allCols[0])) {
            	if(_allCols.length >2){
                	star.addCol(_allCols[1],_allCols[2]);
            	}
            }else if(StringUtils.equals(GENDER,_allCols[0])){
            	if(NumberUtils.toInt(_allCols[1]) == 1){
            		gender.addCol("男", _allCols[2]);
            	}else if(NumberUtils.toInt(_allCols[1]) == 2){
            		gender.addCol("女", _allCols[2]);
            	}else{
            		gender.addCol("性别未知", _allCols[2]);
            	}
            }else if(StringUtils.equals(AGE,_allCols[0])){
            	isAge = true;
            	areaAge.count(NumberUtils.toInt(_allCols[1]));
            }
        	line = dr.readLine();
        }
        if (isAge) {
            for (String key : areaAge.areaKeys) {
                Long value = areaAge.get(key);
                if (value == null) value = 0L;
                age.addCol(key, String.valueOf(value));
            }
        }

        star.sort(Table.SORT_VALUE);
        age.sort(Table.SORT_VALUE);

        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
    }
}
