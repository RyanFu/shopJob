package org.dueam.hadoop.bp.report;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.dueam.hadoop.common.util.Fmt;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/**
 * 美妆产品库来源统计
 */
public class HostPvUvToMeiProduct {
    private static char CTRL_A = (char) 0x01;

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

        Report report = Report.newReport("美妆产品库流量来源统计报表");
        long minNum = 5;
        Map<Integer,Long> sum7Map = new TreeMap<Integer,Long>();

        Table oneWeek = report.newTable("oneWeek", "一周中用户的访问频率统计");
        Table assignedFromAssigned = report.newTable("assignedFromAssigned", "特定页面来自特定来源的流量统计");

        Table tablePv = report.newGroupTable("pv", "list&detail页面的PV统计");
        Table tableUv = report.newGroupTable("uv", "list&detail页面的UV统计");

        Table detailPv = report.newGroupTable("detailPv", "detail页面的PV统计");
        Table detailUv = report.newGroupTable("detailUv", "detail页面的UV统计");
        //Table detailFromIndex = report.newTable("detailFromIndex", "detail页面来自产品库首页的PV-UV统计");
       // Table detailFromList = report.newTable("detailFromList", "detail页面来自list页的流量统计");

        Table listPv = report.newGroupTable("listPv", "list页面的PV统计");
        Table listUv = report.newGroupTable("listUv", "list页面的UV统计");
        //Table listFromIndex = report.newTable("listFromIndex", "list页面来自产品库首页的PV-UV统计");
       /* Table fromIndex = report.newTable("index", "list&detail页面来自产品库首页的PV-UV统计");
        Table detailFromAssigned = report.newTable("detailFromAssigned", "detail页面来自特定来源的流量统计");
        Table listFromAssigned = report.newTable("listFromAssigned", "list页面来自特定来源的流量统计");*/


        for (String line : Utils.readWithCharset(input, "utf-8")) {
            String[] _allCols = StringUtils.splitPreserveAllTokens(line, CTRL_A);
            if (_allCols.length < 2) continue;
            String key = null;
            if(StringUtils.equals(_allCols[0],"f-index")){//list&detail来自产品库首页的流量统计
                assignedFromAssigned.addCol("list&detail来自首页的PV", _allCols[1]);
                assignedFromAssigned.addCol("list&detail来自首页的UV", _allCols[2]);
        	}else if(StringUtils.equals(_allCols[0],"detail")){//detail页面的流量统计
        		if (NumberUtils.toLong(_allCols[2]) > minNum) {
            		key = _allCols[1];
                    if (StringUtils.isEmpty(key)) key = "-";
                    detailPv.addCol(key, _allCols[2]);
            		detailUv.addCol(key, _allCols[3]);
                }
        	}else if(StringUtils.equals(_allCols[0],"detail-f-list")){//detail来自list的流量统计
        		assignedFromAssigned.addCol("detail页面来自list页的PV", _allCols[1]);
        		assignedFromAssigned.addCol("detail页面来自list页的UV", _allCols[2]);
        	}else if(StringUtils.equals(_allCols[0],"detail-f-index")){//detail来自产品库首页的流量统计
        		assignedFromAssigned.addCol("detail页面来自首页的PV", _allCols[1]);
        		assignedFromAssigned.addCol("detail页面来自首页的UV", _allCols[2]);

        	}else if(StringUtils.equals(_allCols[0],"detail-sum")){//detail总的PV-UV统计
        		assignedFromAssigned.addCol("detail页面总PV", _allCols[1]);
        		assignedFromAssigned.addCol("detail页面总UV", _allCols[2]);
        	}else if(StringUtils.equals(_allCols[0],"list")){//list页面的流量统计
        		if (NumberUtils.toLong(_allCols[2]) > minNum) {
            		key = _allCols[1];
                    if (StringUtils.isEmpty(key)) key = "-";
                    listPv.addCol(key, _allCols[2]);
                    listUv.addCol(key, _allCols[3]);
                }
        	}else if(StringUtils.equals(_allCols[0],"list-f-index")){//list来自产品库首页的流量统计
        		assignedFromAssigned.addCol("list页面来自首页的PV", _allCols[1]);
        		assignedFromAssigned.addCol("list页面来自首页的UV", _allCols[2]);
        	}else if(StringUtils.equals(_allCols[0],"list-sum")){//list总的PV-UV统计
        		assignedFromAssigned.addCol("list页面总PV", _allCols[1]);
        		assignedFromAssigned.addCol("list页面总UV", _allCols[2]);
        	}else if (StringUtils.equals(_allCols[0],"day7")) {//一周内用户的粘性统计
            	if(StringUtils.equals("uv",_allCols[1])){
            		int visitDay = Integer.parseInt(_allCols[2]);
            		long sum = Long.parseLong(_allCols[3]);
            		if(sum7Map.size()>0){
                		Iterator iterator = sum7Map.keySet().iterator();
            			while(iterator.hasNext()){
            				int key1 = (Integer)iterator.next();
            				if(key1 < visitDay){
            					sum7Map.put(key1,(Long)sum7Map.get(key1)+ sum);
            				}
            			}
            		}
            		if(sum7Map.get(visitDay)==null){
            			sum7Map.put(visitDay, sum);
            		}
               }
            }else{//list&detail页面的流量统计
        		if (NumberUtils.toLong(_allCols[1]) > minNum) {
                    key = _allCols[0];
                    if (StringUtils.isEmpty(key)) key = "-";
            		tablePv.addCol(key, _allCols[1]);
                    tableUv.addCol(key, _allCols[2]);
                }
        	}
        }
        //一周内用户的粘性统计
		for(int i = 0;i<sum7Map.size();i++){
			int day = i+1;
			oneWeek.addCol("访问频率"+day + "天-uv", sum7Map.get(i+1).toString());
		}

        oneWeek.sort(Table.SORT_KEY);
        tablePv.sort(Table.SORT_VALUE);
        tableUv.sort(Table.SORT_VALUE);
        detailPv.sort(Table.SORT_VALUE);
        detailUv.sort(Table.SORT_VALUE);
        listPv.sort(Table.SORT_VALUE);
        listUv.sort(Table.SORT_VALUE);

        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
    }
}
