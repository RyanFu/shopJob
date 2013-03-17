package org.dueam.hadoop.common.tables;

import org.dueam.hadoop.common.Area;

/**
 * 退款表
 * User: windonly
 * Date: 11-6-29 下午2:49
 */
public class TcRefundTrade {
    public final static int refund_id = 0;// NUMBER 22 N 退款号
    public final static int biz_order_id = 1;// NUMBER 22 N 淘宝交易号
    public final static int out_pay_id = 2;// VARCHAR2 32 N 支付宝交易号
    public final static int return_fee = 3;// NUMBER 22 N 退款金额(买家可得到金额)
    public final static int total_fee = 4;// NUMBER 22 N 交易总金额
    public final static int refund_type = 5;// NUMBER 22 N "退款类型 1：全退给买家 2：部分退给买家(包括退给买家的为0)。 原来设计是给前后台查询用的，但现在好像没用到，现在设为@deprecated，可以考虑去掉"
    public final static int need_return_goods = 6;// NUMBER 22 N 0 "是否需要退货 0：不需要退货 1：需要退货"
    public final static int gmt_create = 7;// DATE 7 N 创建日期
    public final static int gmt_modified = 8;// DATE 7 N 更新日期
    public final static int gmt_agreement_modified = 9;// NUMBER 22 Y 退款协议修改时间戳(精确到毫秒，用long来表示时间)
    public final static int gmt_xiaoer_inter = 10;// DATE 7 Y 小二介入时间
    public final static int gmt_timeout = 11;// DATE 7 Y "超时时间: 如果timeout_type为STOP_TIMEOUT，gmt_timeout为null"
    public final static int timeout_type = 12;// VARCHAR2 40 Y "超时类型 STOP_TIMEOUT：停止超时 WAIT_SELLER_AGREE_TIME_OUT：等待卖家响应协议超时 BUYER_MODIFY_AGREEMENT_TIME_OUT：等待买家修改退款协议超时 WAIT_BUYER_RETURN_GOODS_TIME_OUT：等待买家退货超时 WAIT_SELLER_CONFIRM_GOODS_TIME_OUT：等待卖家确认收货超时"
    public final static int punishment_result = 13;// VARCHAR2 200 Y 小二对投诉的处罚结果
    public final static int refund_status = 14;// NUMBER 22 N 0 "1：退款协议等待卖家确认。2：退款协议已经达成，等待买家退货。3：买家已退货，等待卖家确认收货。4：退款关闭。5：退款成功 6：卖家不同意协议，等待买家修改。"
    public final static int return_goods_status = 15;// NUMBER 22 N 0 "货物状态 1：买家未收到货 2：买家已收到货 3：买家已退货"
    public final static int cs_status = 16;// NUMBER 22 N 0 "客服介入状态 1：不需客服介入 2：需要客服介入 3：客服已经介入处理中 4：客服初审完成 5：客服主管复审失败 6：客服处理完成"
    public final static int buyer_id = 17;// NUMBER 22 N 买家Id
    public final static int seller_id = 18;// NUMBER 22 N 卖家Id
    public final static int buyer_nick = 19;// VARCHAR2 32 N 买家昵称(冗余字段)
    public final static int seller_nick = 20;// VARCHAR2 32 N 卖家昵称(冗余字段)
    public final static int seller_real_name = 21;// VARCHAR2 32 Y 卖家真实姓名(退货用)
    public final static int seller_address = 22;// VARCHAR2 200 Y 卖家退货地址
    public final static int seller_post = 23;// VARCHAR2 20 Y 卖家邮编
    public final static int seller_tel = 24;// VARCHAR2 20 Y 卖家电话
    public final static int seller_mobile = 25;// VARCHAR2 20 Y 卖家手机
    public final static int buyer_logistics_type = 26;// NUMBER 22 Y 退货物流类型 100：平邮 200：快递
    public final static int buyer_logistics_name = 27;// VARCHAR2 200 Y 退货物流公司名称
    public final static int buyer_logistics_mailno = 28;// VARCHAR2 40 Y 退货运单号
    public final static int refund_reason_id = 29;// NUMBER 22 Y 买家退款原因Id，由TM定义
    public final static int refund_reason_text = 30;// VARCHAR2 100 Y 买家退款原因文本
    public final static int refund_desc = 31;// VARCHAR2 400 Y 买家退款说明
    public final static int gmt_buyer_return_goods = 32;// DATE 7 Y 买家退货时间
    public final static int goods_url = 33;// VARCHAR2 256 Y 商品URL(冗余字段)
    public final static int auction_title = 34;// VARCHAR2 60 Y 商品标题(冗余字段)
    public final static int pay_order_id = 35;// NUMBER 22 Y 支付订单ID，为了以后方面汇总查询；初始化成biz_order_id
    public final static int refund_point = 36;// NUMBER 22 Y 退的积分的钱
    public final static int refund_coupon = 37;// NUMBER 22 Y 退的红包的钱
    public final static int biz_type = 38;// NUMBER 22 Y *
    public final static int attributes = 39;// VARCHAR2 4000 Y 存放垫付金额，分账成分金额
    public final static int attributes_cc = 40;// NUMBER 22 Y 更新attributes时的锁
    public final static int advance_status = 41;// NUMBER 22 Y 先行垫付的状态

     public static Area refundCastTimeArea =  Area.newArea(Area.TimeCallback,0,2 * 3600, 12 * 3600 , 24 * 3600 , 2 * 24 * 3600 , 5 * 24 *3600 , 7*24 * 3600 , 14 * 24 * 3600  )  ;
}
