package org.dueam.hadoop.bp.report;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.dueam.hadoop.common.Area;
import org.dueam.hadoop.common.CounterMap;
import org.dueam.hadoop.common.tables.RFactSellPV;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.hadoop.services.ShopDomain;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

import java.beans.PropertyVetoException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 女装卖家统计
 */
public class Ipv {
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

        Report report = Report.newReport(args[1] + "IPV - UV 来源分析");


        CounterMap cMap = CounterMap.newCounter("C2C-PV");
        CounterMap bMap = CounterMap.newCounter("B2C-PV");
        CounterMap cUvMap = CounterMap.newCounter("C2C-UV");
        CounterMap bUvMap = CounterMap.newCounter("B2C-UV");

        for (String line : Utils.readWithCharset(input, "utf-8")) {
            String[] _allCols = StringUtils.splitPreserveAllTokens(line, CTRL_A);
            if (_allCols.length < 3) continue;
            int bizType = NumberUtils.toInt(_allCols[0], 0);
            long pv = NumberUtils.toLong(_allCols[3]);
            long uv = NumberUtils.toLong(_allCols[4]);
            if (StringUtils.isEmpty(_allCols[1])) {
                _allCols[1] = "-";
            }
            if (StringUtils.isEmpty(_allCols[2])) {
                _allCols[2] = "-";
            }
            if (bizType == 1) {
                bMap.add("total", pv);
                bUvMap.add("total", uv);
                common(bMap, bUvMap, _allCols[1], _allCols[2], pv, uv);
            } else {
                cMap.add("total", pv);
                cUvMap.add("total", uv);
                common(cMap, cUvMap, _allCols[1], _allCols[2], pv, uv);
            }
        }
        if (true) {
            Table table = report.newTable("c2c-total", args[1] + "IPV - UV一览");
            table.addCol("pv", String.valueOf(cMap.get("total")));
            table.addCol("uv", String.valueOf(cUvMap.get("total")));
            table.addCol("pv-in", "淘宝内部带来的PV流量", String.valueOf(cMap.get("in-taobao")));
            table.addCol("uv-in", "淘宝内部带来的UV流量", String.valueOf(cUvMap.get("in-taobao")));
            table.addCol("pv-outer", "外部带来的PV流量", String.valueOf(cMap.get("outer-taobao")));
            table.addCol("uv-outer", "外部带来的UV流量", String.valueOf(cUvMap.get("outer-taobao")));
            table.addCol("pv-shop", "店铺带来的PV流量", String.valueOf(cMap.get("in-shop")));
            table.addCol("uv-shop", "店铺带来的UV流量", String.valueOf(cUvMap.get("in-shop")));


        }
        if (true) {
            Table table = report.newGroupTable("c2c-source-pv", args[1] + "IPV来源分布");
            for (Map.Entry<String, AtomicLong> entry : cMap.entrySet()) {
                if (StringUtils.startsWith(entry.getKey(), "group") || "in-shop".equals(entry.getKey()) || "total".equals(entry.getKey()) || "in-taobao".equals(entry.getKey()) || "outer-taobao".equals(entry.getKey()))
                    continue;
                table.addCol(entry.getKey(), String.valueOf(entry.getValue()));
            }
            table.sort(Table.SORT_VALUE);
        }

        if (true) {
            Table table = report.newGroupTable("c2c-source-uv", args[1] + "IPV_UV来源分布");
            for (Map.Entry<String, AtomicLong> entry : cUvMap.entrySet()) {
                if ("total".equals(entry.getKey()) || "in-taobao".equals(entry.getKey()) || "in-shop".equals(entry.getKey()) || "outer-taobao".equals(entry.getKey()))
                    continue;
                table.addCol(entry.getKey(), String.valueOf(entry.getValue()));
            }
            table.sort(Table.SORT_VALUE);
        }
        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
    }

    static Set<String> tbHost = new HashSet<String>();
    static Set<String> commonHost = new HashSet<String>();

    static {
        tbHost.add("taobao.com");
        tbHost.add("etao.com");
        tbHost.add("tmall.com");
        commonHost.add("s.taobao.com");
        commonHost.add("search.taobao.com");
        commonHost.add("s.etao.com");
        commonHost.add("search.etao.com");
        commonHost.add("item.taobao.com");
        commonHost.add("item.tmall.com");
        commonHost.add("upload.taobao.com");
        commonHost.add("bangpai.taobao.com");
        commonHost.add("login.taobao.com");
        commonHost.add("buy.taobao.com");
        commonHost.add("jie.taobao.com");
        commonHost.add("shopXX.taobao.com");
        commonHost.add("www.tmall.com");
        commonHost.add("www.taobao.com");
        commonHost.add("love.taobao.com");
        commonHost.add("shopsearch.taobao.com");
        commonHost.add("bbs.taobao.com");
        commonHost.add("favorite.taobao.com");
        commonHost.add("re.taobao.com");
        commonHost.add("s8.taobao.com");
        commonHost.add("click.taobao.com");
        commonHost.add("ju.taobao.com");
        commonHost.add("fushi.taobao.com");
        commonHost.add("lz.taobao.com");
        commonHost.add("mm.taobao.com");
        commonHost.add("rate.taobao.com");
        commonHost.add("sell.taobao.com");
        commonHost.add("list.tmall.com");
        commonHost.add("8.etao.com");
        commonHost.add("huabao.taobao.com");
        commonHost.add("lp.taobao.com");
        commonHost.add("pianyi.taobao.com");
        commonHost.add("tianxia.taobao.com");
        commonHost.add("tejia.taobao.com");
        commonHost.add("cart.taobao.com");
        commonHost.add("xinpin.taobao.com");
        commonHost.add("i.taobao.com");
        commonHost.add("hers.taobao.com");
        commonHost.add("top.taobao.com");
        commonHost.add("try.taobao.com");
        commonHost.add("trade.taobao.com");
        commonHost.add("wuliu.taobao.com");
        commonHost.add("store.taobao.com");
        commonHost.add("vip.taobao.com");
        commonHost.add("fenxiao.taobao.com");
        commonHost.add("fx.taobao.com");
        commonHost.add("sale.taobao.com");
        commonHost.add("list.taobao.com");
        commonHost.add("list1.taobao.com");


    }

    public static void common(CounterMap cMap, CounterMap cUvMap, String host, String url, long pv, long uv) {
        if (tbHost.contains(host)) {
            if (commonHost.contains(url)) {
                cMap.add(url, pv);
                cUvMap.add(url, uv);
                if ("shopXX.taobao.com".equals(url) || "store.taobao.com".equals(url)) {
                    cUvMap.add("in-shop", uv);
                    cMap.add("in-shop", pv);
                }
            } else if (ShopDomain.isShopDomain(Utils.getSubDomain(url))) {
                if (uv < 100) {
                    cMap.add("UV小于100的店铺", pv);
                    cUvMap.add("UV小于100的店铺", uv);
                } else if (uv < 1000) {
                    cMap.add("UV大于100但小于1000的店铺", pv);
                    cUvMap.add("UV大于100但小于1000的店铺", uv);
                } else if (uv < 5000) {
                    cMap.add("UV大于1000但小于5000的店铺", pv);
                    cUvMap.add("UV大于1000但小于5000的店铺", uv);
                } else if (uv < 10000) {
                    cMap.add("UV大于5000但小于10000的店铺", pv);
                    cUvMap.add("UV大于5000但小于10000的店铺", uv);
                } else {
                    cMap.add("UV大于10000的店铺", pv);
                    cUvMap.add("UV大于10000的店铺", uv);
                }
                cUvMap.add("in-shop", uv);
                cMap.add("in-shop", pv);
            } else {
                if (uv < 100) {
                    cMap.add("引流小于100UV的淘宝二级域名", pv);
                    cUvMap.add("引流小于100UV的淘宝二级域名", uv);
                } else {
                    cMap.add(host, pv);
                    cUvMap.add(host, uv);
                }
            }
            cUvMap.add("in-taobao", uv);
            cMap.add("in-taobao", pv);
        } else {
            cMap.add("outer-taobao", pv);
            cUvMap.add("outer-taobao", uv);
            if (uv < 500) {
                cMap.add("引流小于500UV的外部网站", pv);
                cUvMap.add("引流小于500UV的外部网站", uv);
            } else {
                cMap.add(host, pv);
                cUvMap.add(host, uv);
            }
        }
    }
}
