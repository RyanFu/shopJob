package org.dueam.hadoop.bp.report;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

/**
 * list2.0ɸѡ���ʹ�����ͳ�Ʊ���
 */
public class FilterFormOfZlistReport {
	private static char CTRL_A = (char) 0x01;
	public static final Map<String,String> filterFormMap = new HashMap<String, String>();
	static{
		filterFormMap.put("xie3bao=1","Ь������");
		filterFormMap.put("promoted_service8=tag","���緢��");
		filterFormMap.put("ps32=32","24Сʱ����");
		filterFormMap.put("promoted_service16=16","30��ά��");
		filterFormMap.put("promoted_service4=4","7���˻�");
		filterFormMap.put("auto_post=1","�Զ�����");
		filterFormMap.put("ptdc=1","ƽ̨����");
		filterFormMap.put("appointment=1","ԤԼ��Ϣ");
		filterFormMap.put("bsq=1","Ʒ����Ȩ");
		filterFormMap.put("is3C=1","������");
		filterFormMap.put("support_xcard=1","���ÿ�");
		filterFormMap.put("support_cod=1","��������");
		filterFormMap.put("combo=1","�������");
		filterFormMap.put("onsale_bonusid=1:101;2:103","�ּ�ȯ");
		filterFormMap.put("deli_setup=1","���Ͱ�װ����");
		filterFormMap.put("promoted_service2=tag","��һ����");
		filterFormMap.put("yfxian=1","�˷���");
		filterFormMap.put("limitPromotion=true","��ɱ");
		filterFormMap.put("is_oncharity=1:102","���汦��");
		filterFormMap.put("qsfood=1","QSʳƷ��ȫ");
		filterFormMap.put("user_type=1","��Ʒ����");
		filterFormMap.put("promote=2097152","������Ʒ");
		filterFormMap.put("game_js=1","���۽���");
		filterFormMap.put("offpayment=1","������Ϣ");
		filterFormMap.put("sr=1","�����Ƽ�");
		filterFormMap.put("dtsp=1","ϸ��ʵ��");
		filterFormMap.put("onsale_actcard=1:101;2:120","�Ա�����");
		filterFormMap.put("isprepay=1","�����߱���");
		filterFormMap.put("onsale_vipcard=1:101;2:119;","VIP");
		filterFormMap.put("wtr=1","�����Ƽ�");
		filterFormMap.put("olu=yes","��������");
		filterFormMap.put("third_qc=1","�������ʼ�");
		filterFormMap.put("game_db=1","��������");
		filterFormMap.put("offpayment=1","��������֧��");
		filterFormMap.put("appointment=1","ԤԼ��Ϣ");
		filterFormMap.put("ec=1","���Ӷһ�ȯ");
		filterFormMap.put("closeup=1","ϸ����д");
		filterFormMap.put("exp=1","123ʱЧ");
		filterFormMap.put("photo=1","����Ӱ");
		filterFormMap.put("viewIndex=4","����");
		filterFormMap.put("viewIndex=6","����");
		filterFormMap.put("viewIndex=10","�̳�");
		filterFormMap.put("viewIndex=11","��Ʒ");
		filterFormMap.put("viewIndex=1","ȫ��");
	}
    public static void main(String[] args) throws IOException {
        String input = args[0];
        if (!new File(input).exists()) {
            System.out.println("File Not Exist ! => " + input);
            return;
        }
        Report report = Report.newReport("list2.0ɸѡ���ʹ�����ͳ��");
        Table filterFormTable = report.newTable("filterForm", "ɸѡ���ʹ�����ͳ��");

        for (String line : Utils.readWithCharset(input, "utf-8")) {
            String[] _allCols = StringUtils.splitPreserveAllTokens(line, CTRL_A);
            if (_allCols.length > 2) {
            	String key = _allCols[0]+_allCols[1];
            	if(StringUtils.isNotEmpty(filterFormMap.get(key))){
            		filterFormTable.addCol(key,_allCols[2]);
            	}
            }
        }
        filterFormTable.sort(Table.SORT_VALUE);
        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
    }
}