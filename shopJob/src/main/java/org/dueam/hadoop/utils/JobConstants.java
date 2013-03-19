package org.dueam.hadoop.utils;


/**
 * job常量配置
 * @author zhuliu 2010-11-21
 *
 */
public class JobConstants {
	
	//文件名列表:forest文件/可调属性文件/微调区间
	public static String FOREST_FILENAME = "ForestStdCatDO.data";	//forest
	//public static String FOREST_FILENAME = "ForestStdCatDO-"+ JobHelper.getCurrentDayForForest()+".data";	//forest
	public static String AJUSTPROP_FILENAME = "sccp_config.txt";	//可调属性
	public static String EDITEDDATA_FILENAME = JobConstants.TEMP_DIR + "/sccp_modified.txt";			//微调的输入
	public static String CAT_PRICE_FILENAME = "job2output.data";			//job2的输出:类目的价格分布
	//配置目录就行了
	public static String CAT_PRICE_OLD = JobConstants.TEMP_DIR + "/catjob5olddata";				//老的类目价格的5组数据
	
	//中间文件输出目录
	public static String TEMP_DIR = "/group/tbauction/mengka.hyy/sccp";
	
	//可调属性分隔符列表
	public static final String AJUSTPROP_SP1 = " ";
	public static final String AJUSTPROP_SP2 = "_";
	public static final String AJUSTPROP_SP3 = ":";
	public static final String AJUSTPROP_SP4 = ";";
	public static final int AJUSTPROP_LENGTH = 2;
	
	//订单表的写死序号等等
	public static final String ORDER_SP = "\t";			//订单分隔符
	public static final int ORDER_AUCTION_ID = 8;		//商品id
	public static final int ORDER_CREATE_TIME = 17;		//创建时间
	public static final int ORDER_STATUES = 15;			//订单表状态
	public static final int ORDER_PRICE = 11;			//成交价格
	public static final int ORDER_STATUES_SUCCESS = 2;	//订单拍下已付款
	
	//IPV表的写死序号等等
	public static final String IPV_SP = "\t";			//商品分隔符
	public static final int IPV_AUCTION_ID = 1;			//商品id
	public static final int IPV_AMOUNT = 8;				//IPV数
	
	//退款表的写死序号等等
	public static final String REFUND_SP = "\t";		//商品分隔符
	public static final int REFUND_AUCTION_ID = 1;		//商品id
	public static final int REFUND_AMOUNT = 7;			//退款总次数
	
	//商品表的一些写死的序号等等
	public static final String AUCTION_SP = "\u0001";	//商品分隔符
	public static final int AUCTION_CAT_INDEX = 9;		//类目
	public static final int AUCTION_PV_INDEX = 11;		//商品PV
	public static final int AUCTION_PRICE_INDEX = 5;	//价格
	public static final int AUCTION_TYPE = 8;			//一口价
	public static final int STUFF_STATUS = 10;			//全新
	public static final int AUCTION_STATUS = 4;			//在售
	public static final int AUCTION_ID = 18;			//商品数字id
	public static final int USER_TYPE = 17;				//用户类型：0是C卖家   1是B卖家
	public static final int OPTIONS = 15;				//商品的options
	public static final int USER_ID = 30;				//用户id
	
	//商品必须符合的状态数据(CPV job1)
	public static final String AUCTION_YIKOUJIA = "b";	//一口价
	public static final String AUCTION_NEW = "5";		//全新
	public static final String AUCTION_ONLINE_0 = "0";	//在线0
	public static final String AUCTION_ONLINE_1 = "1";	//在线1
	public static final String AUCTION_OF_9 = "-9";	//CC
	
	//CPV价格区间最终结果是以空格作为分隔符的
	public static final String CPV_PRICE_SP = " ";		//分隔符
	
	//其他
	public static final int TEMP_LENGTH = 2;			//中间输出的默认的长度-2
	public static final String BLANK_SP = "\t";			//中间输出的默认的k-v分隔符-tab
	public static final String COMMON_SP1 = "_";		//中间输出的默认的v的分隔符- _
	public static final String COMMON_SP2 = "-";		//中间输出的默认的v的分隔符- -
	
	//计算类目价格分布(按100一段进行分割)
	public static final double DEFAULT_COUNT = 10.0;	//默认分十段
	public static final double INTERVAL_PRICE = 100.0;	//确定类目价格时,区间段是100元
	public static final double ALLOW_PERCENT = 0.95;	//确定类目价格时,符合95%的区间ok
	public static final double MAX_CIRCLE = 10000;		//最大循环到100w
	
	//统计类目商品总数
	public static final String CAT_AUCTION_SUM_KEY = "_AMOUNT";			//统计类目商品总数的key的后缀
}
