package org.dueam.hadoop.utils;

import org.dueam.hadoop.utils.uic.UicUtils;
import org.dueam.hadoop.utils.uic.UserDO;

public class Sample {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		for(String line : TEXT.split("\n")){
			UserDO user = UicUtils.getUser(line, UicUtils.NUM_NICK);
			System.out.println(user.getId()+","+user.getNick());
		}

	}

	public static String TEXT =
		"only官方旗舰店\n" +
		"百丽官方旗舰店\n" +
		"kappa旗舰\n" +
		"波司登官方旗舰店\n" +
		"九牧王官方旗舰\n" +
		"美特斯邦威官方网店\n" +
		"达芙妮官方旗舰店\n" +
		"双立人官方旗舰店\n" +
		"adidas官方旗舰店\n" +
		"jackjones官方旗舰\n" +
		"美宝莲官方旗舰店\n" +
		"罗莱家纺官方旗舰店\n" +
		"李宁官方网络旗舰店\n" +
		"爱慕官方旗舰店\n" +
		"莎莎官方旗舰店\n" +
		"猫人服饰旗舰店\n" +
		"唐狮官方旗舰店\n" +
		"esprit思创专卖店\n" +
		"森马官方旗舰店\n" +
		"jasonwood旗舰店\n" +
		"堡狮龙旗舰店\n" +
		"以纯官方旗舰店\n" +
		"TonyJeans旗舰店\n" +
		"雅鹿旗舰店\n" +
		"迪士尼时尚旗舰店\n" +
		"真维斯官方旗舰店\n" +
		"passionbreak旗舰店\n" +
		"mecity官方旗舰店\n" +
		"sportica旗舰店\n" +
		"gxg官方旗舰店\n" +
		"veromoda官方旗舰店\n" +
		"osa品牌服饰旗舰店\n" +
		"太平鸟官方专营店\n" +
		"歌莉娅官方旗舰店\n" +
		"ochirly官方旗舰店\n" +
		"秋水伊人官方旗舰店\n" +
		"moco官方旗舰店\n" +
		"othermix旗舰店\n" +
		"ten官方旗舰店\n" +
		"sophy橡菲旗舰店\n" +
		"lapargay官方旗舰店\n" +
		"茵曼旗舰店\n" +
		"andostore安都专卖\n" +
		"szstyle女装旗舰店\n" +
		"idfix旗舰店\n" +
		"容舫商贸法国棒棒糖\n" +
		"麦包包箱包专营店\n" +
		"gnious旗舰店\n" +
		"七匹狼公司旗舰店\n" +
		"太平鸟男装旗舰店\n" +
		"马克华菲官方旗舰店\n" +
		"卡宾官方旗舰\n" +
		"红豆名品服饰旗舰店\n" +
		"稻草人官方旗舰店\n" +
		"varsden旗舰店\n" +
		"deeremarchi旗舰店\n" +
		"sevnjoen66旗舰店\n" +
		"sportica旗舰店\n" +
		"零号男朗亦专卖店\n" +
		"nautica宝尊专卖店\n" +
		"justyle旗舰店\n" +
		"akseries旗舰店\n" +
		"七匹狼皮具北联专卖\n" +
		"格利派蒙旗舰店\n" +
		"威戈官方旗舰店\n" +
		"他她官方旗舰店\n" +
		"红蜻蜓品牌旗舰店\n" +
		"星期六官方旗舰店\n" +
		"利信达箱包专营店\n" +
		"胜道户外专营店\n" +
		"天美意官方旗舰店\n" +
		"ninewest玖熙旗舰店\n" +
		"千百度官方旗舰店\n" +
		"森达官方旗舰店\n" +
		"康奈官方旗舰店\n" +
		"奥康鞋业旗舰店\n" +
		"意尔康皮鞋旗舰店\n" +
		"gracegift旗舰店\n" +
		"羊皮堂男鞋专营店\n" +
		"卡芙琳旗舰店\n" +
		"波伦尼特旗舰店\n" +
		"涉趣旗舰店\n" +
		"arizin旗舰店\n" +
		"anta官方旗舰店\n" +
		"探路者官方旗舰店\n" +
		"胜道户外专营店\n" +
		"名鞋库运动休闲专营\n" +
		"newbalance旗舰店\n" +
		"美津浓官方旗舰店\n" +
		"特步官方旗舰店\n" +
		"鸿星尔克erke旗舰店\n" +
		"匹克官方旗舰店\n" +
		"乔丹官方旗舰店\n" +
		"乐卡克官方旗舰店\n" +
		"feiyue官方旗舰店\n" +
		"361度官方旗舰\n" +
		"cba国辉专卖店\n" +
		"hitec有家专卖店\n" +
		"kingcamp百锐胜途店\n" +
		"eamkevc旗舰店\n" +
		"kailas四服专卖店\n" +
		"博洋家纺旗舰店\n" +
		"奈士迪旗舰店\n" +
		"水星旗舰店\n" +
		"富安娜官方旗舰店\n" +
		"多喜爱旗舰店\n" +
		"梦洁旗舰店\n" +
		"九洲鹿床品旗舰店\n" +
		"百思寒羽绒床品\n" +
		"南极人官方旗舰店\n" +
		"朵彩服饰旗舰店\n" +
		"纤丝鸟旗舰店\n" +
		"北极绒官方旗舰店\n" +
		"gainreel内衣旗舰店\n" +
		"爱美丽官方旗舰店\n" +
		"曼妮芬官方旗舰店\n" +
		"婷美官方旗舰店\n" +
		"雪俐旗舰店\n" +
		"牛尔京城之霜旗舰店\n" +
		"欧莱雅官方旗舰店\n" +
		"曼秀雷敦旗舰店\n" +
		"mg百库专卖店\n" +
		"相宜本草丽人丽妆\n" +
		"爱仕达厨具旗舰店\n" +
		"膳魔师旗舰店\n" +
		"多样屋家居旗舰店\n" +
		"康宁官方旗舰店\n" +
		"雅高旗舰店\n" +
		"迪士尼官方旗舰店\n" +
		"巴布豆官方旗舰店\n" +
		"可可鸭官方旗舰店\n" +
		"爱制造旗舰店\n" +
		"摩登小姐旗舰店\n" +
		"喜梦宝官方旗舰店\n" +
		"奥汀堡艺术家具\n" +
		"美亿佳旗舰店\n" +
		"奥朵家饰用品旗舰店\n" +
		"圣象官方旗舰店\n" +
		"那沃旗舰店\n" +
		"尚客茶品旗舰店\n" +
		"麦克棉花布包旗舰店\n" +
		"special品牌旗舰店";


}
