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
 * Time: ����8:11
 * fineryList �첽�������
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
        Report report = Report.newReport("List2.0����ͳ��");
        Table detailTable = report.newViewTable("detailTable", "������ݻ���");
        Table tradeTable = report.newViewTable("tradeTable", "�������ݻ���");
        Table totalTable = report.newTable("total", "�������ݻ���");
        Table catTable = report.newTable("cat", "��Ŀ�������");
        Table propertyTable = report.newTable("property", "���Ե������");
        Table filterTable = report.newTable("filter", "��������ص������");
        Table auctionTable = report.newTable("auction", "��Ʒ���������");
        Table pageTable = report.newTable("page", "ҳ��������");

        int totalPv = 0, totalUv = 0,
                totalPv1 = 0, totalPv2 = 0,    //ͬ�����pv 1 ���첽���pv 2
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
                        catTable.addCol("marketHomePv", "�г���ҳ���Pv", pv);
                        catTable.addCol("marketHomeUv", "�г���ҳ���Uv", uv);
                        catPv = catPv + Integer.parseInt(pv);
                        catUv = catUv + Integer.parseInt(uv);
                    }
                    if (StringUtils.equals(subPosition, "2")) {
                        catTable.addCol("catPathpv", "·��������pv", pv);
                        catTable.addCol("catPathuv", "·��������uv", uv);
                        catPv = catPv + Integer.parseInt(pv);
                        catUv = catUv + Integer.parseInt(uv);
                    }
                    if (StringUtils.equals(subPosition, "3")) {
                        catTable.addCol("catPv", "����Ŀ�ͷ�����Pv", pv);
                        catTable.addCol("catUv", "����Ŀ�ͷ�����Uv", uv);
                        catPv = catPv + Integer.parseInt(pv);
                        catUv = catUv + Integer.parseInt(uv);
                    }
                    if (StringUtils.equals(subPosition, "4")) {
                        catTable.addCol("moreCatpv", "��Ŀ������һ�����pv", pv);
                        catTable.addCol("moreCatuv", "��Ŀ������һ�����uv", uv);
                    }
                    if (StringUtils.equals(subPosition, "5")) {
                        catTable.addCol("nPagepv", "��һ���л�pv", pv);
                        catTable.addCol("nPageuv", "��һ���л�uv", uv);
                    }
                    if (StringUtils.equals(subPosition, "6")) {
                        catTable.addCol("ppagepv", "��һ���л�pv", pv);
                        catTable.addCol("ppageuv", "��һ���л�uv", uv);
                    }
                } else if (StringUtils.contains(position, "/hesper.51.")) {
                    propertyTable.addCol("propertypv" + subPosition, "����pv" + subPosition, pv);
                    propertyTable.addCol("propertyuv" + subPosition, "����uv" + subPosition, uv);
                    if (!StringUtils.equals(subPosition, "100")) {
                        propertyPv = propertyPv + Integer.parseInt(pv);
                        propertyUv = propertyUv + Integer.parseInt(uv);
                    }
                } else if (StringUtils.contains(position, "/hesper.52.")) {
                    if (StringUtils.equals(subPosition, "1")) {
                        auctionTable.addCol("samepv", "�ϲ�ͬ��pv", pv);
                        auctionTable.addCol("sameuv", "�ϲ�ͬ��uv", uv);
                        auctionPv = auctionPv + Integer.parseInt(pv);
                        auctionUv = auctionUv + Integer.parseInt(uv);
                    }
                    if (StringUtils.equals(subPosition, "2")) {
                        auctionTable.addCol("favoritepv", "�����ղ�pv", pv);
                        auctionTable.addCol("favoriteuv", "�����ղ�uv", uv);
                    }


                } else if (StringUtils.contains(position, "/hesper.53.")) {
                    if (StringUtils.equals(subPosition, "1")) {
                        pageTable.addCol("selfPagepv", "����ҳ��pv", pv);
                        pageTable.addCol("selfPageuv", "����ҳ��uv", uv);
                    }
                    if (StringUtils.equals(subPosition, "2")) {
                        pageTable.addCol("prePagepv", "��һҳ���pv", pv);
                        pageTable.addCol("prePageuv", "��һҳ���uv", uv);
                    }
                    if (StringUtils.equals(subPosition, "3")) {
                        pageTable.addCol("nextPagepv", "��һҳ���pv", pv);
                        pageTable.addCol("nextPageuv", "��һҳ���uv", uv);
                    }
                    if (StringUtils.equals(subPosition, "4")) {
                        pageTable.addCol("somPagepv", "�����ʾҳ��pv", pv);
                        pageTable.addCol("somPageuv", "�����ʾҳ��uv", uv);
                    }
                    pagePv = pagePv + Integer.parseInt(pv);
                    pageUv = pageUv + Integer.parseInt(uv);

                } else if (StringUtils.contains(position, "/hesper.54.")) {
                    if (StringUtils.equals(subPosition, "1")) {
                        filterTable.addCol("servicepv", "���������pv", pv);
                        filterTable.addCol("serviceuv", "���������uv", uv);
                    }
                    if (StringUtils.equals(subPosition, "2")) {
                        filterTable.addCol("sortpv", "���������pv", pv);
                        filterTable.addCol("sortuv", "���������uv", uv);
                    }
                    if (StringUtils.equals(subPosition, "3")) {
                        filterTable.addCol("locationpv", "����ɸѡpv", pv);
                        filterTable.addCol("locationuv", "����ɸѡuv", uv);
                    }
                    if (StringUtils.equals(subPosition, "4")) {
                        filterTable.addCol("somPagepv", "�۸�����pv", pv);
                        filterTable.addCol("somPageuv", "�۸�����uv", uv);
                    }
                    if (StringUtils.equals(subPosition, "5")) {
                        filterTable.addCol("somPagepv", "�ؼ�������pv", pv);
                        filterTable.addCol("somPageuv", "�ؼ�������uv", uv);
                    }
                    filterPv = filterPv + Integer.parseInt(pv);
                    filterUv = filterUv + Integer.parseInt(uv);
                }
            }

        }

        totalPv2 = catPv + propertyPv + filterPv + auctionPv + pagePv;
        totalPv = totalPv1 + totalPv2;
        String pvRate = Fmt.div(totalPv, uvMid);  // pv/��
        String detailRate = Fmt.parent2(String.valueOf(ipvUv), String.valueOf(totalUv));//���ת����
        String moneyPerson = Fmt.div(totalMoney, buyerUv);  //�͵���
        String buyRate = Fmt.parent2(String.valueOf(buyerUv), String.valueOf(ipvUv));

        detailTable.
                addCol("����").
                addCol("PV").
                addCol("UV").
                addCol("UV-MID").
                addCol("IPV").
                addCol("IPV-UV").
                addCol("���ת����").
                addCol("pv/��").
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
                addCol("����").
                addCol("֧�������").
                addCol("IPV-UV").
                addCol("����UV").
                addCol("����ת����").
                addCol("������").
                addCol("�͵���").
                addCol(Report.BREAK_VALUE);

        tradeTable.
                addCol(domain).
                addCol(String.valueOf(totalMoney)).
                addCol(String.valueOf(ipvUv)).
                addCol(String.valueOf(buyerUv)).
                addCol(String.valueOf(buyRate)).     //����ץ����
                addCol(String.valueOf(orderCount)).
                addCol(String.valueOf(moneyPerson)).
                addCol(Report.BREAK_VALUE);

        totalTable.addCol("totalPv", "PV", String.valueOf(totalPv));
        totalTable.addCol("uvmid", "UV-MID", String.valueOf(uvMid));
        totalTable.addCol("pvRate", "PV/��(����100)", String.valueOf(totalPv*100/uvMid));
        totalTable.addCol("totalPv1", "ͬ�����PV", String.valueOf(totalPv1));
        totalTable.addCol("totalPv2", "�첽���PV", String.valueOf(totalPv2));
        totalTable.addCol("totaluv", "��UV", String.valueOf(totalUv));
        totalTable.addCol("totalmoney", "�ܳɽ���", String.valueOf(totalMoney));
        totalTable.addCol("moneyPerson", "�͵���(����100)", String.valueOf(totalMoney*100/buyerUv));
        totalTable.addCol("itemUv", "�н��׵���Ʒ", String.valueOf(itemUv));
        totalTable.addCol("sellerUv", "�н��׵�����", String.valueOf(sellerUv));
        totalTable.addCol("ipvUv","IPV-UV" ,String.valueOf(ipvUv)) ;
        totalTable.addCol("detailRate","���ת����*10000" ,String.valueOf(ipvUv*10000/totalUv)) ;
        totalTable.addCol("buyRate","����ת����*10000" ,String.valueOf(buyerUv*10000/ipvUv)) ;
        
        totalTable.addCol("catPv", "��Ŀ���", String.valueOf(catPv));
        totalTable.addCol("filterPv", "�����ɸѡ��PV", String.valueOf(filterPv));
        totalTable.addCol("propertyPv", "��������PV", String.valueOf(propertyPv));
        totalTable.addCol("auctionPv", "��Ʒ���첽���PV", String.valueOf(auctionPv));
        totalTable.addCol("pagePv", "ҳ��������PV", String.valueOf(pagePv));

        totalTable.addCol("totalUv", "�첽���UV", String.valueOf(totalUv));
        totalTable.addCol("catUv", "��Ŀ���UV", String.valueOf(catUv));
        totalTable.addCol("filterUv", "�����ɸѡ��UV", String.valueOf(filterUv));
        totalTable.addCol("propertyUv", "��������UV", String.valueOf(propertyUv));
        totalTable.addCol("auctionUv", "��Ʒ���첽���UV", String.valueOf(auctionUv));
        totalTable.addCol("pageUv", "ҳ��������UV", String.valueOf(pageUv));

        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
    }
}
