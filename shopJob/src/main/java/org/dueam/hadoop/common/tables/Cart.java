package org.dueam.hadoop.common.tables;

import org.apache.commons.lang.StringUtils;

public class Cart {
    public static int CART_ID = 0;//	NUMBER	N			购物车Id
    public static int OUTER_ID = 1;//	NUMBER	N			外部ID：商品Id或者SkuId
    public static int OUTER_ID_TYPE = 2;//	NUMBER	N			"外部ID类型1：商品ID2：SKUID"
    public static int QUANTITY = 3;//	NUMBER	N			数量
    public static int LAST_MODIFIED = 4;//	DATE	N	sysdate		最后更新时间，用于购物车里面的排序，添加到购物车的时候更新，包括添加相同商品或者SKU到购物车
    public static int ATTRIBUTE = 5;//	VARCHAR2(1024)	Y			用于存放附件信息，例如套餐ID列表
    public static int USER_ID = 6;//	NUMBER	Y	-1		用户ID
    public static int TRACK_ID = 7;//	VARCHAR2(32)	Y			CookieID，用于访客购物车
    public static int STATUS = 8;//	NUMBER	N	1		"状态1：正常 ：  -1：购物车下单删除    -2：合并购物车删除    -3：买家主动删除"
    public static int XID = 9;//	VARCHAR2(10)	N			商品/SKU分库，减少查询
    public static int GMT_CREATE = 10;//	DATE	N	sysdate		属性创建时间
    public static int GMT_MODIFIED = 11;//	DATE	N	sysdate		属性修改时间
    public static int ATTRIBUTE_CC = 12;//	NUMBER	Y
    public static int SHOP_ID = 13;//	NUMBER	Y			店铺id
    public static int AUCTION_ID = 14;//	NUMBER	Y			商品id

    public static boolean isDeleted(String[] _allCols) {
        return "-1".equals(_allCols[STATUS]) || "-2".endsWith(_allCols[STATUS]) || "-3".equals(_allCols[STATUS]);
    }

    /**
     * 转换status状态
     *
     * @param status
     * @return
     */
    public static String status(String status) {
        String statusName = "";
        if ("-1".equals(status)) {
            statusName = "购物车下单删除";
        } else if ("-2".equals(status)) {
            statusName = "合并购物车删除";
        } else if ("-3".equals(status)) {
            statusName = "买家主动删除";
        } else if ("1".endsWith(status)) {
            statusName = "正常";
        }
        return statusName + "[" + status + "]";
    }
}
