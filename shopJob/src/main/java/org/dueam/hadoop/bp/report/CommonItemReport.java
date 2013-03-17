package org.dueam.hadoop.bp.report;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.dueam.hadoop.common.util.Fmt;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

/**
 * 通用商品报表
 */
public class CommonItemReport {
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

        String name = "";
        if (args.length > 1) {
            name = args[1];
        }

        Report report = Report.newReport(name + "商品交易报表");
        Map<String, Table> summayMap = new HashMap<String, Table>();
        summayMap.put("all", report.newTable("allsummay", "全网--基本详情"));

        for (String line : Utils.readWithCharset(input, "utf-8")) {
            String[] allCols = StringUtils.splitPreserveAllTokens(line, CTRL_A);

            if ("summay".equals(allCols[0])) {

                Table summay = getTable(summayMap, allCols[1], report);

                summay.addCol("商品数", allCols[2]);
                summay.addCol("在线商品数", allCols[3]);
                summay.addCol("新发商品数", allCols[4]);
                summay.addCol("包邮商品数", allCols[5]);
                continue;
            }

            if ("ipv".equals(allCols[0])) {

                Table summay = getTable(summayMap, allCols[1], report);

                summay.addCol("IPV", allCols[2]);
                summay.addCol("IPV-UV", allCols[3]);
                continue;
            }

            if ("trade".equals(allCols[0])) {

                Table summay = getTable(summayMap, allCols[1], report);

                if (summay == null) {
                    continue;
                }

                summay.addCol("支付宝金额", StringUtils.isNumeric(allCols[2]) ? allCols[2] : "0");
                summay.addCol("发生购买的商品数", allCols[3]);
                summay.addCol("发生购买的买家数", allCols[4]);
                summay.addCol("发生购买的卖家数", allCols[5]);
                summay.addCol("订单数", allCols[6]);
                summay.addCol("客单价", Fmt.parent4(allCols[2], allCols[4]));
                continue;
            }

            if ("memeber_cart".equals(allCols[0])) {

                Table summay = getTable(summayMap, allCols[1], report);

                if (summay == null) {
                    continue;
                }

                summay.addCol("加入购物车总次数(会员)", allCols[2]);
                continue;
            }

            if ("guest_cart".equals(allCols[0])) {

                Table summay = getTable(summayMap, allCols[1], report);

                if (summay == null) {
                    continue;
                }
                summay.addCol("加入购物车总次数(访客)", allCols[2]);
                continue;
            }

        }

        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
    }

    private static Table getTable(Map<String, Table> summayMap, String catId, Report report) {

        Table summay = summayMap.get(catId);

        String catName = catMap.get(catId);

        if (summay == null) {
            summay = report.newTable(catId + "summay", catName == null ? catId : catName + "--基本详情");
            summayMap.put(catId, summay);
        }
        return summay;
    }

    /**
     * 一级类目map
     */
    private static Map<String, String> catMap = new HashMap<String, String>();

    static {

        catMap.put("40", "腾讯QQ专区");
        catMap.put("1512", "手机");
        catMap.put("50008163", "床上用品/布艺软饰");
        catMap.put("50011397", "珠宝/钻石/翡翠/黄金");
        catMap.put("50016348", "清洁/卫浴/收纳/整理用具");
        catMap.put("50018222", "台式机/一体机/服务器");
        catMap.put("50016349", "厨房/餐饮用具");
        catMap.put("50013886", "户外/登山/野营/旅行用品");
        catMap.put("50020276", "品牌保健品");
        catMap.put("50019379", "合作商家");
        catMap.put("50020808", "家居饰品");
        catMap.put("50025705", "洗护清洁剂/卫生巾/纸/香薰");
        catMap.put("50023904", "国货精品数码");
        catMap.put("50025004", "个性定制/设计服务/DIY");
        catMap.put("50025111", "本地化生活服务");
        catMap.put("50025490", "本地生活团购");
        catMap.put("50025706", "贵金属");
        catMap.put("29", "宠物/宠物食品及用品");
        catMap.put("1201", "MP3/MP4/iPod/录音笔");
        catMap.put("1101", "笔记本电脑");
        catMap.put("34", "音乐/影视/明星/音像");
        catMap.put("33", "书籍/杂志/报纸");
        catMap.put("30", "男装");
        catMap.put("28", "ZIPPO/瑞士军刀/眼镜");
        catMap.put("26", "汽车/用品/配件/改装/摩托");
        catMap.put("99", "网络游戏点卡");
        catMap.put("50011699", "运动服/运动包/颈环配件");
        catMap.put("50008075", "吃喝玩乐折扣券");
        catMap.put("50010788", "彩妆/香水/美妆工具");
        catMap.put("50008907", "手机号码/套餐/增值业务");
        catMap.put("50008164", "住宅家具");
        catMap.put("50018004", "电子词典/电纸书/文化用品");
        catMap.put("50018252", "电子凭证");
        catMap.put("50022703", "大家电");
        catMap.put("50023282", "美发护发/假发");
        catMap.put("50019780", "平板电脑/MID");
        catMap.put("50020332", "基础建材");
        catMap.put("50020857", "特色手工艺");
        catMap.put("50024186", "保险");
        catMap.put("50023717", "OTC药品/医疗器械/隐形眼镜/计生用品");
        catMap.put("50024612", "外卖/外送/订餐服务（垂直市场）");
        catMap.put("2813", "成人用品/避孕/计生用品");
        catMap.put("35", "奶粉/辅食/营养品");
        catMap.put("23", "古董/邮币/字画/收藏");
        catMap.put("21", "居家日用/婚庆/创意礼品");
        catMap.put("20", "电玩/配件/游戏/攻略");
        catMap.put("16", "女装/女士精品");
        catMap.put("14", "数码相机/单反相机/摄像机");
        catMap.put("11", "电脑硬件/显示器/电脑周边");
        catMap.put("50005700", "品牌手表/流行手表");
        catMap.put("1625", "女士内衣/男士内衣/家居服");
        catMap.put("50011972", "影音电器");
        catMap.put("50011949", "特价酒店/特色客栈/公寓旅馆");
        catMap.put("50010404", "服饰配件/皮带/帽子/围巾");
        catMap.put("50008165", "童装/童鞋/亲子装");
        catMap.put("50008090", "3C数码配件市场");
        catMap.put("50014812", "尿片/洗护/喂哺/推车床");
        catMap.put("50016891", "网游垂直市场根类目");
        catMap.put("50014442", "交通票");
        catMap.put("50017652", "TP服务商大类");
        catMap.put("50022517", "孕妇装/孕产妇用品/营养");
        catMap.put("50020579", "电子/电工");
        catMap.put("50020611", "商业/办公家具");
        catMap.put("50024971", "新车/二手车");
        catMap.put("25", "玩具/模型/动漫/早教/益智");
        catMap.put("27", "家装主材");
        catMap.put("50002768", "个人护理/保健/按摩器材");
        catMap.put("50004958", "移动/联通/电信充值中心");
        catMap.put("50011740", "流行男鞋");
        catMap.put("50007218", "办公设备/耗材/相关服务");
        catMap.put("50012029", "运动鞋new");
        catMap.put("1801", "美容护肤/美体/精油");
        catMap.put("50007216", "鲜花速递/花卉仿真/绿植园艺");
        catMap.put("50011665", "网游装备/游戏币/帐号/代练");
        catMap.put("50016422", "粮油/蔬果/水产/速食");
        catMap.put("50013864", "饰品/流行首饰/时尚饰品新");
        catMap.put("50014811", "网店/网络服务/软件");
        catMap.put("50020275", "传统滋补品/其他保健营养品");
        catMap.put("50025968", "司法拍卖拍品专用");
        catMap.put("50026316", "茶/酒/冲饮");
        catMap.put("50023724", "其他");
        catMap.put("50024449", "淘花娱乐");
        catMap.put("50023575", "房产/租房/新房/二手房/委托服务");
        catMap.put("50025110", "电影/演出/体育赛事");
        catMap.put("50024451", "外卖/外送/订餐服务");
        catMap.put("50023878", "自用闲置转让");
        catMap.put("50002766", "零食/坚果/茶叶/特产");
        catMap.put("50006842", "箱包皮具/热销女包/男包");
        catMap.put("50006843", "女鞋");
        catMap.put("50012100", "生活电器");
        catMap.put("50012082", "厨房电器");
        catMap.put("50012164", "闪存卡/U盘/存储/移动硬盘");
        catMap.put("50010728", "运动/瑜伽/健身/球迷用品");
        catMap.put("50017908", "彩票");
        catMap.put("50018264", "网络设备/网络相关");
        catMap.put("50017300", "乐器/吉他/钢琴/配件");
        catMap.put("50020485", "五金/工具");
        catMap.put("50025707", "景点门票/度假线路/旅游服务");
        catMap.put("50025618", "基金");
        catMap.put("50023804", "家居服务");

    }

}
