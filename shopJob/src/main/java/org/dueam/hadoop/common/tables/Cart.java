package org.dueam.hadoop.common.tables;

import org.apache.commons.lang.StringUtils;

public class Cart {
    public static int CART_ID = 0;//	NUMBER	N			���ﳵId
    public static int OUTER_ID = 1;//	NUMBER	N			�ⲿID����ƷId����SkuId
    public static int OUTER_ID_TYPE = 2;//	NUMBER	N			"�ⲿID����1����ƷID2��SKUID"
    public static int QUANTITY = 3;//	NUMBER	N			����
    public static int LAST_MODIFIED = 4;//	DATE	N	sysdate		������ʱ�䣬���ڹ��ﳵ�����������ӵ����ﳵ��ʱ����£����������ͬ��Ʒ����SKU�����ﳵ
    public static int ATTRIBUTE = 5;//	VARCHAR2(1024)	Y			���ڴ�Ÿ�����Ϣ�������ײ�ID�б�
    public static int USER_ID = 6;//	NUMBER	Y	-1		�û�ID
    public static int TRACK_ID = 7;//	VARCHAR2(32)	Y			CookieID�����ڷÿ͹��ﳵ
    public static int STATUS = 8;//	NUMBER	N	1		"״̬1������ ��  -1�����ﳵ�µ�ɾ��    -2���ϲ����ﳵɾ��    -3���������ɾ��"
    public static int XID = 9;//	VARCHAR2(10)	N			��Ʒ/SKU�ֿ⣬���ٲ�ѯ
    public static int GMT_CREATE = 10;//	DATE	N	sysdate		���Դ���ʱ��
    public static int GMT_MODIFIED = 11;//	DATE	N	sysdate		�����޸�ʱ��
    public static int ATTRIBUTE_CC = 12;//	NUMBER	Y
    public static int SHOP_ID = 13;//	NUMBER	Y			����id
    public static int AUCTION_ID = 14;//	NUMBER	Y			��Ʒid

    public static boolean isDeleted(String[] _allCols) {
        return "-1".equals(_allCols[STATUS]) || "-2".endsWith(_allCols[STATUS]) || "-3".equals(_allCols[STATUS]);
    }

    /**
     * ת��status״̬
     *
     * @param status
     * @return
     */
    public static String status(String status) {
        String statusName = "";
        if ("-1".equals(status)) {
            statusName = "���ﳵ�µ�ɾ��";
        } else if ("-2".equals(status)) {
            statusName = "�ϲ����ﳵɾ��";
        } else if ("-3".equals(status)) {
            statusName = "�������ɾ��";
        } else if ("1".endsWith(status)) {
            statusName = "����";
        }
        return statusName + "[" + status + "]";
    }
}
