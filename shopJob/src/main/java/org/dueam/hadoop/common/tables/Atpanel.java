package org.dueam.hadoop.common.tables;

public interface Atpanel {
	int time =0 ; // 访问时间，yyyyMMddhhmmss 中
	int ip =1 ; // 访问者ip低
	int refer =2 ; // 访问当前的一系列参数，以"&"进行分隔，格式为"参数名=参数值"。如：pre=访问来源url 高
	int mid =3 ; // 访问者机器id 高
	int uid =4 ; // 会员id 高
	int sid =5 ; // session id 低
	int aid =6 ; // 不详，原本是广告id的，但未被使用，等同于占位符 无
	int url =7 ; // 当前访问的url高
	int agent =8 ; // 访问代理信息，如操作系统、浏览器等 低
	int adid =9 ; // 广告id,前2位数字做类型分类,从10开始代表如下 : 10 线上推广 11 邮件引导 12 搜索引擎 中
	int amid =10; // ALL_TAOBAO_MARKET_ID 全网营销id,独立于ad_id 中
	int cmid =11; // CHANNEL_MARKET_ID 频道营销id 中
	int pmid =12; // PLACE_MARKET_ID 位置营销代码,可以独立于ad_id之外 中
	int nmid =13; // 从cookie中获取的，由4个字段组成：adid:mid:sessionid:time 低
	int nuid   =14; // 从cookie中获取的，由4个字段组成：adid:uid:sessionid:time 低
	int channelid =15; // 频道id 低
}
