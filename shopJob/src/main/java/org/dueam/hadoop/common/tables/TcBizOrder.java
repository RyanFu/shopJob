package org.dueam.hadoop.common.tables;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.dueam.hadoop.common.util.MapUtils;
import org.dueam.hadoop.common.util.Utils;

import java.util.Map;

/**
 * 订单表
 *
 * @author suoni
 */
public class TcBizOrder {
    public static int BIZ_ORDER_ID = 0;
    public static int PAY_ORDER_ID = 1;
    public static int LOGISTICS_ORDER_ID = 2;
    public static int OUT_ORDER_ID = 3;
    public static int BUYER_NICK = 4;
    public static int SELLER_NICK = 5;
    public static int BUYER_ID = 6;
    public static int SELLER_ID = 7;
    public static int AUCTION_ID = 8;
    public static int SNAP_PATH = 9;
    public static int AUCTION_TITLE = 10;
    public static int AUCTION_PRICE = 11;
    public static int BUY_AMOUNT = 12;
    public static int BIZ_TYPE = 13;
    public static int MEMO = 14;
    public static int PAY_STATUS = 15;
    public static int LOGISTICS_STATUS = 16;
    public static int GMT_CREATE = 17;
    public static int GMT_MODIFIED = 18;
    public static int STATUS = 19;
    public static int SUB_BIZ_TYPE = 20;
    public static int FAIL_REASON = 21;
    public static int GOODS_URL = 22;
    public static int BUYER_RATE_STATUS = 23;
    public static int SELLER_RATE_STATUS = 24;
    public static int SELLER_MEMO = 25;
    public static int BUYER_MEMO = 26;
    public static int SELLER_FLAG = 27;
    public static int BUYER_FLAG = 28;
    public static int BUYER_MESSAGE_PATH = 29;
    public static int REFUND_STATUS = 30;
    public static int ATTRIBUTES = 31;
    public static int ATTRIBUTES_CC = 32;
    public static int IP = 33;
    public static int END_TIME = 34;
    public static int PAY_TIME = 35;
    public static int AUCTION_PICT_URL = 36;
    public static int IS_DETAIL = 37;
    public static int IS_MAIN = 38;
    public static int POINT_RATE = 39;
    public static int PARENT_ID = 40;
    public static int ADJUST_FEE = 41;
    public static int DISCOUNT_FEE = 42;
    public static int REFUND_FEE = 43;
    public static int CONFIRM_PAID_FEE = 44;
    public static int COD_STATUS = 45;
    public static int GMT_TIMEOUT = 46;

    /**
     * return total fee ( as cent ) : only in main order
     *
     * @param _allCols all cols of order
     * @return
     */
    public static long getTotalFee(String[] _allCols) {
        if (isMain(_allCols)) {
            String tf = Utils.getValue(_allCols[TcBizOrder.ATTRIBUTES], "tf", "0");
            return NumberUtils.toLong(tf, 0);
        } else {
            long auctionPrice = (long) NumberUtils.toDouble(
                    _allCols[TcBizOrder.AUCTION_PRICE]);
            long buyAmount = (long) NumberUtils.toDouble(_allCols[TcBizOrder.BUY_AMOUNT]);
            long adjustFee = (long) NumberUtils.toDouble(
                    _allCols[TcBizOrder.ADJUST_FEE]);
            long discountFee = (long) NumberUtils.toDouble(
                    _allCols[TcBizOrder.DISCOUNT_FEE]);
            long refundFee = (long) NumberUtils.toDouble(
                    _allCols[TcBizOrder.REFUND_FEE]);

            long amount = auctionPrice * buyAmount + adjustFee
                    - discountFee - refundFee;
            return amount;
        }


    }

    /**
     * get sub type of item ( as item,auction,group )
     *
     * @param _allCols
     * @return
     */
    public static String getSubType(String[] _allCols) {
        String subType = _allCols[TcBizOrder.SUB_BIZ_TYPE];
        if ("1".equals(subType)) {
            return "item";
        } else if ("2".equals(subType)) {
            return "auction";
        } else if ("3".equals(subType)) {
            return "group";
        }
        return "item";
    }

    public static boolean isEffective(String[] _allCols) {
        return "0".equals(_allCols[TcBizOrder.STATUS]);
    }

    public static boolean isMain(String[] _allCols) {
        return "1".equals(_allCols[TcBizOrder.IS_MAIN]);
    }

    public static boolean isDetail(String[] _allCols) {
        return "1".equals(_allCols[TcBizOrder.IS_DETAIL]);
    }

    public static boolean isPaied(String[] _allCols) {
        if ("2".equals(_allCols[TcBizOrder.PAY_STATUS])
                || "6".equals(_allCols[TcBizOrder.PAY_STATUS])) {
            return true;
        }
        return false;
    }

    private static Map<String, String> BIZ_TYPE_MAP = MapUtils.asMap(new String[]{"100", "直冲",
            "200", "团购拍卖一口价",
            "300", "自动发货",
            "500", "货到付款",
            "600", "外部网店(标准版)",
            "610", "外部网店入门版",
            "620", "外部网店(shopEX版)",
            "630", "淘宝与万网合作版网",
            "650", "外部网店统一版",
            "700", "旅游产品",
            "710", "酒店",
            "800", "分销平台(采购单)",
            "900", "网游虚拟交易"});

    public static String getBizType(String type) {
        String value = BIZ_TYPE_MAP.get(type);
        return (value == null ? "" : value) + "[" + type + "]";
    }


    public static void main(String[] args) {
        String input = "直冲 100， \n" +
                "团购拍卖一口价 200，\n" +
                "自动发货 300， \n" +
                "货到付款 500， \n" +
                "外部网店(标准版) 600， \n" +
                "外部网店入门版 610，\n" +
                "外部网店(shopEX版) 620，\n" +
                "淘宝与万网合作版网 630，\n" +
                "外部网店统一版 650 ，\n" +
                "旅游产品 700， \n" +
                "酒店 710，\n" +
                "分销平台(采购单) 800， \n" +
                "网游虚拟交易 900";
        for (String line : input.split("\n")) {
            String key = StringUtils.strip(line, "， ").split(" ")[1];
            String value = StringUtils.strip(line, "， ").split(" ")[0];
            System.out.println("\"" + key + "\"," + "\"" + value + "\",");
        }
    }

    public static String bizType(String type) {

        return null;
    }

    /**
     * is b2c order
     *
     * @param _allCols
     * @return
     */
    public static boolean isB2C(String[] _allCols) {
        return "1".equals(Utils.getValue(_allCols[TcBizOrder.ATTRIBUTES], "isB2C", "0"));
    }

    /**
     * 是否处于待付款状态
     */
    public static boolean noPaid(String[] _allCols){
        return "0".equals(_allCols[STATUS]) && ("1".equals(_allCols[PAY_STATUS]) ||"7".equals(_allCols[PAY_STATUS])  );
    }

     /**
     * 是否还未确认收货
     */
    public static boolean noCom(String[] _allCols){
        return "0".equals(_allCols[STATUS]) && "2".equals(_allCols[LOGISTICS_STATUS]) && ("2".equals(_allCols[PAY_STATUS])   );
    }

    /**
     * 是否还未确认收货
     */
    public static boolean noRate(String[] _allCols){
        return "0".equals(_allCols[STATUS]) && "5".equals(_allCols[BUYER_RATE_STATUS]) && ("6".equals(_allCols[PAY_STATUS])  );
    }

    /**
     * 允许的属性key
     */
    public static final String[] ALLOW_ATTRIBUTE_KEY = new String[]{
            "wap", //是否是wap
            "bShow", //是否参与了buyerShow
            "vip", //是否vip打折
            "ppay", //支持先行赔付
            "suitBuyer", //是否投诉了买家
            "suitSeller", //是否投诉了卖家
            "suitBuyerType",//投诉买家类型
            "suitSellerType",//投诉卖家类型
            "virtual", //是否虛擬物品
            "bnotified", //买家已经提醒卖家
            "snotified", //卖家已经提醒买家
            "CCViewed", //自动发货的卡密是否查看过
            "rootCat", //跟类目
            "shipping", //发货方式，交易中心内部使用
            "isBonus", //是否使用折扣券
            "isCharity", //是否慈善
            "dg", //代购
            "auto2", //第二种发货方式
            "tf", //=payOrderDO.actualTotalFee 总价
            "pf", //=logisticsDO.postFee 邮费
            "df", //=payOrderDO.discountFee 系统折扣
            "tjk", //淘金卡 2008年7月11日 伏威添加， 石头的需求
            "j13", //是否支持假一赔三 2008年8月5日 伏威添加， 石头的需求
            "7d", //是否支持7天无理由退换货 2008年8月5日 伏威添加， 石头的需求
            "qk", //是否支持虚拟物品闪电发货 2008年8月5日 伏威添加， 石头的需求
            "3c3", //是否支持数码与家电30天维修 2008年8月5日 伏威添加， 石头的需求
            "ps", //是否已经处罚卖家 ps = punish seller
            "px",//单品促销活动ID
            "mjj",//买就减，格式是 是否上不封顶（1是0不是）-买就减的阀值-减的现金（单位分）
            "tc",  //套餐的ID
            "sku",  //sku属性
            "isB2C", //是否b2c
            "mjjm",  //店铺促销活动 满就减
            "mjmy",   //店铺促销活动 满就免邮
            "mjslw",  //店铺促销活动 满就送礼物
            "mjsjf",   //店铺促销活动 满就送积分
            "hsp",    //购买当时有促销活动，防止通过uic去判断是否有活动
            "CCSend",  //买家付款后的发卡状态
            "isAP",    //是否淘客交易
            "point",    //使用的积分
            "catId",    //类目id
            "postPaid", //邮费已经退款/确认收货格式: postPaid:bizOrderId
            "outerIdP",   //商家外部编码，IC商品中的字段内容
            "outerIdSKU",   //商家外部编码，IC商品中的字段内容
            "arf",        //交易后退款金额
            "cf",//COD费用
            "ct",//cod买家签收时间
            "cart",//来自购物车
            "top",//交易来自TOP
            "vCat", //该订单是否是属于虚拟类目下的
            "lpRate",//限时打折产生的交易的折扣率
            "ptid",//下单时的运费模板id
            "xcard", //是否支持信用卡,长空信用卡项目
            //网游
            "buyerTel",//买家联系电话(网游虚拟装备用)
            "opWW",//欧飞客服旺旺(网游虚拟装备用)
            "tradeType",//网游虚拟装备交易类型，分为寄售交易，担保交易和帐号交易
            "pwViewed",//网游虚拟装备的帐号是否察看过，察看过的话存放察看时间
            "coop",//合作商名称
            "peifu",//欧飞已赔付
            //网游结束
            "spuid", //阿飞需要的spuid
            //分销
            "bbcid",//分销商品id
            "tprice",//分销商品零售价
            "tbid", //淘宝买家id                    f
            "tbnick", //淘宝买家nick
            "poid",//外部商家编码（商品条码）,
            "skup",//分销商品的sku
            //分销结束
            "fbuy", //表示是快速拍订单
            "secKill",//秒杀
            "codKey",//cod修改价格时支付宝返回的key
            "from",//订单的来源，目前仅保存了3d淘宝，取值为：3d
            "once_t", //是否使用的是一次性的优惠(之魂的需求，im buy )
            "promotion", //优惠id(之魂的需求，im buy )
            "cod3Fee",//cod三家分润,
            "dist_price_range", //采购单零售价区间
            "realRootCat", //存放子订单的根类目ID(added by mulao,实时交易数据分析)
            "zpbz", //支持正品保障
            "instId",//支付宝付款消息返回的机构名称,
            "payTool", //支付宝付款消息返回的支付工具
            "accessSubType", //支付宝付款消息返回的支付渠道
            "instPayAmount", //支付宝付款消息返回的资金金额
            "decreaseStore",//减库存的方式
            "market",//垂直市场名称
            "serialNo",//宝贝序列号
            "ttid", //wap使用,宗潜需求, 特殊标识,表示外部合作网站进来的
            "daifu", //代付订单标记
            "peerPayerId", //代付人的userId，支付宝支付成功消息里带上
            "peerPayerName", //代付人的呢称，支付宝支付成功消息里带上
            "peerPayerLoginId", //代付人的登录id,支付宝支付成功消息里带上
            "shopname",//外店名称
            "newsku",//新修改的sku
            "newskustatus",//新修改的sku状态
            "outerIdNewSKU",//新修改的商家编码
            "shopid",//内店的店铺id
            "lm",//是否修改过物流收货地址
            "topTradeSource", //top使用,风胜需求,TOP创建交易的来源
            "topAppKey", //top使用,风胜需求,TOP应用的编号
            "kltb", //快乐淘宝 樊英需求
            "postTrade",//邮寄标记
            "gt2pf",//无货赔付
            "gt2",//新平台网游交易
            "freezeID",//保证金id
            "prepayCat",//提供给维权平台使用的消保标记
            "oversold", //超卖
            "est", //直冲子业务类型，如手机直充，游戏直充，宽带直充，笑非需求
            "bankfrom", //创建交易时的信用卡银行类别
            "cst", //cod子服务id
            "pxjkc",//商品被设置为"拍下减库存"
            "noGoodsPS", //无货空挂处罚卖家
            "meal", //优惠平台项目 套餐ID 如meal:223323 (子订单上的标记)
            "shopdt", //优惠金额 店铺的折扣价
            "o_promotion", //主订单优惠id
            "o_once_t", //主订单优惠 是否使用的是一次性的优惠
            "lock", //退款先行垫付时对订单的锁定标记，1是锁定，0是未锁定，牧劳需求
            "advStat", //退款先行垫付状态，主要用于订单展示显示其垫付状态，牧劳需求
            "advTotal", //退款先行垫付给买家的总金额，出账成功时写入
            "pn", //punish是否处罚过卖家，钦风需求，added by mulao
            "cartGrp", //分组统计购物车的订单情况，程通需求
            "codPayTime", //cod交易支付宝确认付款时间
            "yfx", //运费险 ，凌宝需求
            "codAF", //cod交易的实付费用（买家实际支付给物流的费用）
            "hotelConfT", //酒店类型交易 从卖家发货到自动确认收货的时间  单位为天
            "alipayrp", //支付宝退款积分
            "refundTime", //退款创建时间，维权需求 骆寒,added by mulao
            "rType", //维权类型，维权需求，骆寒，1,2,3,4几个状态，4表示小二介入过,0表示关闭TC关心
            "itemTag", //商品标签  周健需求
            "anony", //匿名购买 ,给detail和评价展示匿名购买的订单 杜复需求
            "buyerMobile", //买家匿名购买时留下的联系手机号 ,元坎需求
            "activity",//标注活动类型,add by huaidao
            "jhs", //聚划算订单, 采霜需求
            "alipayPoint",//支付时使用的支付宝积分额度
            "AliEA2",//直充商家的第二个支付宝账号（用于佣金扣除） 张风需求

            "kd_p", //跨店铺优惠，辰战需求，以后添加attributes，如果不是显示在已买到和已卖出列表，都一律添加到tc_biz_vertical里
            "yfxFee",//运费险保单金额 需要在已买到展示
            "yfxBizId",//运费险保险订单id 需要在已买到展示
            "addCart",//加入购物车的时间，周健需求

            "cfb", //COD 买家承担的服务费     记于父订单       add by xuezhu
            "cfp", //COD 档位选择用户  是买家还是卖家    0是买家   1是卖家  记于子订单    add by xuezhu
            "cfsp",//COD 卖家承担的百分比  0<=x<=100 整数     记于子订单上     add by xuezhu
            "cfs",  //COD 卖家愿不愿意承担服务费  0是不愿意   1是愿意  记于主订单上    add by xuezhu
            "cfaf", //COD 服务费调整金额 add by xuezhu
            "cfm",  //COD 卖家是否可以修改邮费add by xuezhu
            "cfbi",  //COD 买家承担的服务费初使值add by xuezhu

            "opfx",//opensearch二期返现比率
            "oppd",//opensearch二期订单优惠描述
            "opfxje",//opensearch二期返现金额
            "opappkey",//opensearch站外商家appkey
            "seller_phone",//卖家phone
            "closeAAddSto",//交易关闭后，宝贝库存加回 ，展宁需求 add by baiyan
            "displayprice", //打折商品在成交记录区显示原价的标记 双十一 商城宝贝打五折
            "payChannelCode"//支付前置后用户选择的支付宝付款渠道代码
    };

    /**
     * true这个值
     */
    public static final String ATTRIBUTE_VALUE_TRUE = "1";

    /**
     * 是否是聚划算的订单
     *
     * @return true:是聚划算订单;false:不是聚划算的订单
     */
    public static boolean isFromTgroupon(String[] _allCols) {
        return ATTRIBUTE_VALUE_TRUE
                .equals(Utils.getValue(_allCols[TcBizOrder.ATTRIBUTES], "jhs", "false"))
                || StringUtils.isNotBlank(Utils.getValue(_allCols[TcBizOrder.ATTRIBUTES], "tgType", ""));
    }
}
