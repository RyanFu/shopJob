package org.dueam.hadoop.common.tables;

/**
 * User: windonly
 * Date: 11-7-29 上午11:32
 */
public class RFactSellPV {
    public final static int gmt_create = 0; //string日期
    public final static int id = 1; //string卖家ID
    public final static int nick = 2; //string卖家nick
    public final static int category = 3; //string店铺类目ID
    public final static int category_name = 4; //string店铺类目名
    public final static int ipv = 5; //bigint店铺IPV
    public final static int uv = 6; //bigint店铺UV
    public final static int zfb_total_fee = 7; //double支付宝成交金额
    public final static int zfb_quantity = 8; //bigint支付宝成交笔数
    public final static int zfb_users = 9; //bigint支付宝成交UV
    public final static int product_count = 10; //bigint店铺商品数
    public final static int alipay_rank = 11; //bigint根据成交金额排名
}
