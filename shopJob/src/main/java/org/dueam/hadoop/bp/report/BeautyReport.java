package org.dueam.hadoop.bp.report;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

/**
 * 我是大美人活动统计
 * 
 * @author linyi
 */
public class BeautyReport {
    private static char CTRL_A = (char) 0x01;

    protected static Map<String, String> typeMap = new HashMap<String, String>();
    protected static Map<String, TypeDO> typeDoMap = new TreeMap<String, TypeDO>();

    protected static TypeDO allType = new TypeDO("all");

    static {

        typeMap.put("all", "基本详情");
        typeMap.put("1", "美妆");
        typeMap.put("2", "美肤");
        typeMap.put("3", "美发");

        typeDoMap.put("1", new TypeDO("1"));
        typeDoMap.put("2", new TypeDO("2"));
        typeDoMap.put("3", new TypeDO("3"));

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

        Report report = Report.newReport("我是大美人活动统计");

        Table viewTable = report.newTable("view", "页面浏览");

        String newApplyCount = "";

        for (String line : Utils.readWithCharset(input, "utf-8")) {
            String[] allCols = StringUtils.splitPreserveAllTokens(line, CTRL_A);

            if ("all".equals(allCols[0])) {
                TypeDO type = typeDoMap.get(allCols[1]);
                if (type != null) {
                    type.addGenderTotal(allCols[2], allCols[4]);
                    type.addStatusTotal(allCols[3], allCols[4]);
                    allType.addGenderTotal(allCols[2], allCols[4]);
                    allType.addStatusTotal(allCols[3], allCols[4]);
                }

            } else if ("page".equals(allCols[0])) {

                if ("main".equals(allCols[1])) {
                    addColls(viewTable, "活动页", allCols);
                } else if ("one".equals(allCols[1])) {
                    addColls(viewTable, "报名第一页", allCols);
                } else if ("two".equals(allCols[1])) {
                    addColls(viewTable, "报名第二页", allCols);
                } else if ("lottery".equals(allCols[1])) {
                    addColls(viewTable, "抽奖页", allCols);
                } else if ("dameiren".equals(allCols[1])) {
                    addColls(viewTable, "300强投票页", allCols);
                }else if ("30to12".equals(allCols[1])) {
                    addColls(viewTable, "30进12投票页", allCols);
                }

            } else if ("new".equals(allCols[0])) {
                //每天新增报名数
                newApplyCount = allCols[1];
            }
        }

        Table summary = createTable(report, allType);
        summary.addCol("当天新增报名数", newApplyCount);

        for (TypeDO type : typeDoMap.values()) {
            createTable(report, type);
        }

        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
    }

    private static void addColls(Table table, String prefix, String[] allCols) {

        table.addCol(prefix + "-PV", allCols[2]);
        table.addCol(prefix + "-UV", allCols[3]);
        table.addCol(prefix + "-MID", allCols[4]);

    }

    private static Table createTable(Report report, TypeDO type) {

        Table typeTable = report.newTable(type.getTypeId(), typeMap.get(type.getTypeId()));

        typeTable.addCol("报名人数", type.getTotalStr());
        typeTable.addCol("男性", type.getGenderTotalStr("1"));
        typeTable.addCol("女性", type.getGenderTotalStr("0"));

        typeTable.addCol("海选", type.getStatusTotalStr("0"));
        typeTable.addCol("初赛", type.getStatusTotalStr("1"));
        typeTable.addCol("复赛", type.getStatusTotalStr("2"));
        typeTable.addCol("决赛", type.getStatusTotalStr("3"));
        typeTable.addCol("淘汰", type.getStatusTotalStr("-1"));

        return typeTable;

    }
}

class TypeDO {

    TypeDO(String typeId) {
        this.typeId = typeId;
    }

    private String typeId;

    public String getTypeId() {
        return typeId;
    }

    private Map<String, Integer> genderTotal = new HashMap<String, Integer>();

    private Map<String, Integer> statusTotal = new HashMap<String, Integer>();

    public void addGenderTotal(String gender, int num) {
        if (num > 0) {
            genderTotal.put(gender, num + this.getGenderTotal(gender));
        }
    }

    public void addGenderTotal(String gender, String num) {
        this.addGenderTotal(gender, Integer.valueOf(num));
    }

    public void addStatusTotal(String status, int num) {
        if (num > 0) {
            statusTotal.put(status, num + this.getStatusTotal(status));
        }
    }

    public void addStatusTotal(String gender, String num) {
        this.addStatusTotal(gender, Integer.valueOf(num));
    }

    public int getGenderTotal(String gender) {
        Integer total = genderTotal.get(gender);
        return total == null ? 0 : total;
    }

    public String getGenderTotalStr(String gender) {
        return String.valueOf(this.getGenderTotal(gender));
    }

    public int getStatusTotal(String status) {
        Integer total = statusTotal.get(status);
        return total == null ? 0 : total;
    }

    public String getStatusTotalStr(String status) {
        return String.valueOf(this.getStatusTotal(status));
    }

    public int getTotal() {
        return this.getGenderTotal("0") + this.getGenderTotal("1");
    }

    public String getTotalStr() {
        return String.valueOf(this.getGenderTotal("0") + this.getGenderTotal("1"));
    }

}
