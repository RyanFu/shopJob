package org.dueam.hadoop.bp.report;

import org.apache.commons.lang.StringUtils;
import org.dueam.hadoop.common.util.Fmt;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: fenghao
 * Date: 11-12-22
 * Time: 下午8:11
 * fineryList 异步点击数据
 */
public class FineryReport {

    private static char CTRL_A = (char) 0x01;
    public static void main(String[] args) throws IOException {
        String input = args[0];
        //String input = "d:/file.txt" ;
        if (!new File(input).exists()) {
            System.out.println("File Not Exist ! => " + input);
            return;
        }
        Report report = Report.newReport("List2.0数据统计");
        Table detailTable = report.newViewTable("detailTable", "浏览数据汇总");
        Table tradeTable = report.newViewTable("tradeTable", "交易数据汇总");
        Table totalTable = report.newTable("total", "总体数据汇总");
        Table catTable = report.newTable("cat", "类目点击数据");
        Table propertyTable = report.newTable("property", "属性点击数据");
        Table filterTable = report.newTable("filter", "排序和搜素点击数据");
        Table auctionTable = report.newTable("auction", "商品区点击数据");
        Table pageTable = report.newTable("page", "页码点击数据");

        int totalPv = 0, totalUv = 0,
                totalPv1 = 0, totalPv2 = 0,    //同步点击pv 1 ，异步点击pv 2
                catPv = 0, catUv = 0,
                propertyUv = 0, propertyPv = 0,
                filterUv = 0, filterPv = 0,
                auctionUv = 0, auctionPv = 0,
                pageUv = 0, pagePv = 0,
                ipv = 0, uvMid = 0,
                ipvUv = 0, ipvUvMid = 0,
                totalMoney = 0, buyerUv = 0, orderCount = 0,
                itemUv = 0, sellerUv = 0;
        String domain = "";

        for (String line : Utils.readWithCharset(input, "utf-8")) {
            String[] _allCols = StringUtils.splitPreserveAllTokens(line, CTRL_A);

            if (_allCols.length > 3) {
                if (StringUtils.equals(_allCols[0], "ls")) {
                    domain = _allCols[1];
                    totalPv1 = Integer.parseInt(_allCols[2]);
                    totalUv = Integer.parseInt(_allCols[3]);
                    uvMid = Integer.parseInt(_allCols[4]);
                }
                if (StringUtils.equals(_allCols[0], "ipv")) {
                    domain = _allCols[1];
                    ipv = Integer.parseInt(_allCols[2]);
                    ipvUv = Integer.parseInt(_allCols[3]);
                    ipvUvMid = Integer.parseInt(_allCols[4]);
                }
                if (StringUtils.equals(_allCols[0], "trade")) {
                    domain = _allCols[1];
                    totalMoney = Integer.parseInt(_allCols[2]);
                    itemUv = Integer.parseInt(_allCols[3]);
                    buyerUv = Integer.parseInt(_allCols[4]);
                    sellerUv = Integer.parseInt(_allCols[5]);
                    orderCount = Integer.parseInt(_allCols[6]);
                }

            } else {
                String position = _allCols[0];
                String subPosition = StringUtils.substring(position, 11);
                String uv = _allCols[1];
                String pv = _allCols[2];
                if (StringUtils.contains(position, "/hesper.50.")) {
                    if (StringUtils.equals(subPosition, "1")) {
                        catTable.addCol("marketHomePv", "市场首页点击Pv", pv);
                        catTable.addCol("marketHomeUv", "市场首页点击Uv", uv);
                        catPv = catPv + Integer.parseInt(pv);
                        catUv = catUv + Integer.parseInt(uv);
                    }
                    if (StringUtils.equals(subPosition, "2")) {
                        catTable.addCol("catPathpv", "路径区域点击pv", pv);
                        catTable.addCol("catPathuv", "路径区域点击uv", uv);
                        catPv = catPv + Integer.parseInt(pv);
                        catUv = catUv + Integer.parseInt(uv);
                    }
                    if (StringUtils.equals(subPosition, "3")) {
                        catTable.addCol("catPv", "子类目和分组点击Pv", pv);
                        catTable.addCol("catUv", "子类目和分组点击Uv", uv);
                        catPv = catPv + Integer.parseInt(pv);
                        catUv = catUv + Integer.parseInt(uv);
                    }
                    if (StringUtils.equals(subPosition, "4")) {
                        catTable.addCol("moreCatpv", "类目区超过一屏情况pv", pv);
                        catTable.addCol("moreCatuv", "类目区超过一屏情况uv", uv);
                    }
                    if (StringUtils.equals(subPosition, "5")) {
                        catTable.addCol("nPagepv", "下一屏切换pv", pv);
                        catTable.addCol("nPageuv", "下一屏切换uv", uv);
                    }
                    if (StringUtils.equals(subPosition, "6")) {
                        catTable.addCol("ppagepv", "上一屏切换pv", pv);
                        catTable.addCol("ppageuv", "上一屏切换uv", uv);
                    }
                } else if (StringUtils.contains(position, "/hesper.51.")) {
                    propertyTable.addCol("propertypv" + subPosition, "属性pv" + subPosition, pv);
                    propertyTable.addCol("propertyuv" + subPosition, "属性uv" + subPosition, uv);
                    if (!StringUtils.equals(subPosition, "100")) {
                        propertyPv = propertyPv + Integer.parseInt(pv);
                        propertyUv = propertyUv + Integer.parseInt(uv);
                    }
                } else if (StringUtils.contains(position, "/hesper.52.")) {
                    if (StringUtils.equals(subPosition, "1")) {
                        auctionTable.addCol("samepv", "合并同款pv", pv);
                        auctionTable.addCol("sameuv", "合并同款uv", uv);
                        auctionPv = auctionPv + Integer.parseInt(pv);
                        auctionUv = auctionUv + Integer.parseInt(uv);
                    }
                    if (StringUtils.equals(subPosition, "2")) {
                        auctionTable.addCol("favoritepv", "宝贝收藏pv", pv);
                        auctionTable.addCol("favoriteuv", "宝贝收藏uv", uv);
                    }


                } else if (StringUtils.contains(position, "/hesper.53.")) {
                    if (StringUtils.equals(subPosition, "1")) {
                        pageTable.addCol("selfPagepv", "输入页码pv", pv);
                        pageTable.addCol("selfPageuv", "输入页码uv", uv);
                    }
                    if (StringUtils.equals(subPosition, "2")) {
                        pageTable.addCol("prePagepv", "上一页点击pv", pv);
                        pageTable.addCol("prePageuv", "上一页点击uv", uv);
                    }
                    if (StringUtils.equals(subPosition, "3")) {
                        pageTable.addCol("nextPagepv", "下一页点击pv", pv);
                        pageTable.addCol("nextPageuv", "下一页点击uv", uv);
                    }
                    if (StringUtils.equals(subPosition, "4")) {
                        pageTable.addCol("somPagepv", "点击显示页码pv", pv);
                        pageTable.addCol("somPageuv", "点击显示页码uv", uv);
                    }
                    pagePv = pagePv + Integer.parseInt(pv);
                    pageUv = pageUv + Integer.parseInt(uv);

                } else if (StringUtils.contains(position, "/hesper.54.")) {
                    if (StringUtils.equals(subPosition, "1")) {
                        filterTable.addCol("servicepv", "服务区点击pv", pv);
                        filterTable.addCol("serviceuv", "服务区点击uv", uv);
                    }
                    if (StringUtils.equals(subPosition, "2")) {
                        filterTable.addCol("sortpv", "排序点击点击pv", pv);
                        filterTable.addCol("sortuv", "排序点击点击uv", uv);
                    }
                    if (StringUtils.equals(subPosition, "3")) {
                        filterTable.addCol("locationpv", "地区筛选pv", pv);
                        filterTable.addCol("locationuv", "地区筛选uv", uv);
                    }
                    if (StringUtils.equals(subPosition, "4")) {
                        filterTable.addCol("somPagepv", "价格区间pv", pv);
                        filterTable.addCol("somPageuv", "价格区间uv", uv);
                    }
                    if (StringUtils.equals(subPosition, "5")) {
                        filterTable.addCol("somPagepv", "关键字搜索pv", pv);
                        filterTable.addCol("somPageuv", "关键字搜索uv", uv);
                    }
                    filterPv = filterPv + Integer.parseInt(pv);
                    filterUv = filterUv + Integer.parseInt(uv);
                }
            }

        }

        totalPv2 = catPv + propertyPv + filterPv + auctionPv + pagePv;
        totalPv = totalPv1 + totalPv2;
        String pvRate = Fmt.div(totalPv, uvMid);  // pv/人
        String detailRate = Fmt.parent2(String.valueOf(ipvUv), String.valueOf(totalUv));//浏览转化率
        String moneyPerson = Fmt.div(totalMoney, buyerUv);  //客单价
        String buyRate = Fmt.parent2(String.valueOf(buyerUv), String.valueOf(ipvUv));

        detailTable.
                addCol("域名").
                addCol("PV").
                addCol("UV").
                addCol("UV-MID").
                addCol("IPV").
                addCol("IPV-UV").
                addCol("浏览转化率").
                addCol("pv/人").
                addCol(Report.BREAK_VALUE);
        detailTable.
                addCol(domain).
                addCol(String.valueOf(totalPv)).
                addCol(String.valueOf(totalUv)).
                addCol(String.valueOf(uvMid)).
                addCol(String.valueOf(ipv)).
                addCol(String.valueOf(ipvUv)).
                addCol(String.valueOf(detailRate)).
                addCol(String.valueOf(pvRate)).
                addCol(Report.BREAK_VALUE);

        tradeTable.
                addCol("域名").
                addCol("支付宝金额").
                addCol("IPV-UV").
                addCol("购买UV").
                addCol("购买转化率").
                addCol("订单数").
                addCol("客单价").
                addCol(Report.BREAK_VALUE);

        tradeTable.
                addCol(domain).
                addCol(String.valueOf(totalMoney)).
                addCol(String.valueOf(ipvUv)).
                addCol(String.valueOf(buyerUv)).
                addCol(String.valueOf(buyRate)).     //购买抓化率
                addCol(String.valueOf(orderCount)).
                addCol(String.valueOf(moneyPerson)).
                addCol(Report.BREAK_VALUE);

        totalTable.addCol("totalPv", "PV", String.valueOf(totalPv));
        totalTable.addCol("uvmid", "UV-MID", String.valueOf(uvMid));
        totalTable.addCol("pvRate", "PV/人(除以100)", String.valueOf(totalPv*100/uvMid));
        totalTable.addCol("totalPv1", "同步点击PV", String.valueOf(totalPv1));
        totalTable.addCol("totalPv2", "异步点击PV", String.valueOf(totalPv2));
        totalTable.addCol("totaluv", "总UV", String.valueOf(totalUv));
        totalTable.addCol("totalmoney", "总成交额", String.valueOf(totalMoney));
        totalTable.addCol("moneyPerson", "客单价(除以100)", String.valueOf(totalMoney*100/buyerUv));
        totalTable.addCol("itemUv", "有交易的商品", String.valueOf(itemUv));
        totalTable.addCol("sellerUv", "有交易的卖家", String.valueOf(sellerUv));
        totalTable.addCol("ipvUv","IPV-UV" ,String.valueOf(ipvUv)) ;
        totalTable.addCol("detailRate","浏览转化率*10000" ,String.valueOf(ipvUv*10000/totalUv)) ;
        totalTable.addCol("buyRate","购买转化率*10000" ,String.valueOf(buyerUv*10000/ipvUv)) ;
        
        totalTable.addCol("catPv", "类目点击", String.valueOf(catPv));
        totalTable.addCol("filterPv", "排序和筛选区PV", String.valueOf(filterPv));
        totalTable.addCol("propertyPv", "属性区域PV", String.valueOf(propertyPv));
        totalTable.addCol("auctionPv", "商品区异步点击PV", String.valueOf(auctionPv));
        totalTable.addCol("pagePv", "页码区域点击PV", String.valueOf(pagePv));

        totalTable.addCol("totalUv", "异步点击UV", String.valueOf(totalUv));
        totalTable.addCol("catUv", "类目点击UV", String.valueOf(catUv));
        totalTable.addCol("filterUv", "排序和筛选区UV", String.valueOf(filterUv));
        totalTable.addCol("propertyUv", "属性区域UV", String.valueOf(propertyUv));
        totalTable.addCol("auctionUv", "商品区异步点击UV", String.valueOf(auctionUv));
        totalTable.addCol("pageUv", "页码区域点击UV", String.valueOf(pageUv));

        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
    }
}
