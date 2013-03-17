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
 * ��ױ��Ʒ����Դͳ��
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

        Report report = Report.newReport("��ױ��Ʒ��������Դͳ�Ʊ���");
        long minNum = 5;
        Map<Integer,Long> sum7Map = new TreeMap<Integer,Long>();

        Table oneWeek = report.newTable("oneWeek", "һ�����û��ķ���Ƶ��ͳ��");
        Table assignedFromAssigned = report.newTable("assignedFromAssigned", "�ض�ҳ�������ض���Դ������ͳ��");

        Table tablePv = report.newGroupTable("pv", "list&detailҳ���PVͳ��");
        Table tableUv = report.newGroupTable("uv", "list&detailҳ���UVͳ��");

        Table detailPv = report.newGroupTable("detailPv", "detailҳ���PVͳ��");
        Table detailUv = report.newGroupTable("detailUv", "detailҳ���UVͳ��");
        //Table detailFromIndex = report.newTable("detailFromIndex", "detailҳ�����Բ�Ʒ����ҳ��PV-UVͳ��");
       // Table detailFromList = report.newTable("detailFromList", "detailҳ������listҳ������ͳ��");

        Table listPv = report.newGroupTable("listPv", "listҳ���PVͳ��");
        Table listUv = report.newGroupTable("listUv", "listҳ���UVͳ��");
        //Table listFromIndex = report.newTable("listFromIndex", "listҳ�����Բ�Ʒ����ҳ��PV-UVͳ��");
       /* Table fromIndex = report.newTable("index", "list&detailҳ�����Բ�Ʒ����ҳ��PV-UVͳ��");
        Table detailFromAssigned = report.newTable("detailFromAssigned", "detailҳ�������ض���Դ������ͳ��");
        Table listFromAssigned = report.newTable("listFromAssigned", "listҳ�������ض���Դ������ͳ��");*/


        for (String line : Utils.readWithCharset(input, "utf-8")) {
            String[] _allCols = StringUtils.splitPreserveAllTokens(line, CTRL_A);
            if (_allCols.length < 2) continue;
            String key = null;
            if(StringUtils.equals(_allCols[0],"f-index")){//list&detail���Բ�Ʒ����ҳ������ͳ��
                assignedFromAssigned.addCol("list&detail������ҳ��PV", _allCols[1]);
                assignedFromAssigned.addCol("list&detail������ҳ��UV", _allCols[2]);
        	}else if(StringUtils.equals(_allCols[0],"detail")){//detailҳ�������ͳ��
        		if (NumberUtils.toLong(_allCols[2]) > minNum) {
            		key = _allCols[1];
                    if (StringUtils.isEmpty(key)) key = "-";
                    detailPv.addCol(key, _allCols[2]);
            		detailUv.addCol(key, _allCols[3]);
                }
        	}else if(StringUtils.equals(_allCols[0],"detail-f-list")){//detail����list������ͳ��
        		assignedFromAssigned.addCol("detailҳ������listҳ��PV", _allCols[1]);
        		assignedFromAssigned.addCol("detailҳ������listҳ��UV", _allCols[2]);
        	}else if(StringUtils.equals(_allCols[0],"detail-f-index")){//detail���Բ�Ʒ����ҳ������ͳ��
        		assignedFromAssigned.addCol("detailҳ��������ҳ��PV", _allCols[1]);
        		assignedFromAssigned.addCol("detailҳ��������ҳ��UV", _allCols[2]);

        	}else if(StringUtils.equals(_allCols[0],"detail-sum")){//detail�ܵ�PV-UVͳ��
        		assignedFromAssigned.addCol("detailҳ����PV", _allCols[1]);
        		assignedFromAssigned.addCol("detailҳ����UV", _allCols[2]);
        	}else if(StringUtils.equals(_allCols[0],"list")){//listҳ�������ͳ��
        		if (NumberUtils.toLong(_allCols[2]) > minNum) {
            		key = _allCols[1];
                    if (StringUtils.isEmpty(key)) key = "-";
                    listPv.addCol(key, _allCols[2]);
                    listUv.addCol(key, _allCols[3]);
                }
        	}else if(StringUtils.equals(_allCols[0],"list-f-index")){//list���Բ�Ʒ����ҳ������ͳ��
        		assignedFromAssigned.addCol("listҳ��������ҳ��PV", _allCols[1]);
        		assignedFromAssigned.addCol("listҳ��������ҳ��UV", _allCols[2]);
        	}else if(StringUtils.equals(_allCols[0],"list-sum")){//list�ܵ�PV-UVͳ��
        		assignedFromAssigned.addCol("listҳ����PV", _allCols[1]);
        		assignedFromAssigned.addCol("listҳ����UV", _allCols[2]);
        	}else if (StringUtils.equals(_allCols[0],"day7")) {//һ�����û���ճ��ͳ��
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
            }else{//list&detailҳ�������ͳ��
        		if (NumberUtils.toLong(_allCols[1]) > minNum) {
                    key = _allCols[0];
                    if (StringUtils.isEmpty(key)) key = "-";
            		tablePv.addCol(key, _allCols[1]);
                    tableUv.addCol(key, _allCols[2]);
                }
        	}
        }
        //һ�����û���ճ��ͳ��
		for(int i = 0;i<sum7Map.size();i++){
			int day = i+1;
			oneWeek.addCol("����Ƶ��"+day + "��-uv", sum7Map.get(i+1).toString());
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
