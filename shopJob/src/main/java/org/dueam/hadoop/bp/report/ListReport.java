package org.dueam.hadoop.bp.report;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.dueam.hadoop.common.util.Fmt;
import org.dueam.hadoop.common.util.MapUtils;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

/**
 * Ůװ����ͳ��
 */
public class ListReport {
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

        Report report = Report.newReport(name + "LIST & SEARCH ת���ʱ���");

        Map<String, List<String[]>> today = MapUtils.map(Utils.read(input), CTRL_A);
        Map<String, String[]> ipvMap = MapUtils.toMap(today.get("ipv"));
        Map<String, String[]> tradeMap = MapUtils.toMap(today.get("trade"));
        Table total = report.newViewTable("total", "LIST & SEARCH ��������");
        Table trade = report.newViewTable("trade", "LIST & SEARCH ��������(�������)");
        Table detail = report.newTable("detail", "LIST & SEARCH �������");
        total.addCol("����").addCol("PV").addCol("UV����Ա��")
                /*.addCol("UV��MID��")*/.addCol("IPV").addCol("IPV-UV")
                /*.addCol("IPV-UV��MID��")*/.addCol("���ת����").addCol("PV/��")
                .addCol(Report.BREAK_VALUE);
        trade.addCol("����").addCol("֧�������").addCol("IPV-UV")
                /*.addCol("UV��MID��")*/.addCol("����UV").addCol("����ת����")
                /*.addCol("IPV-UV��MID��")*/.addCol("������").addCol("�͵���")
                .addCol(Report.BREAK_VALUE);
        for (String[] cols : today.get("ls")) {
            if (!HOSTS.contains(cols[0])) continue;

            detail.addCol(cols[0] + "-PV", cols[1]);
            detail.addCol(cols[0] + "-UV����Ա��", cols[2]);
            detail.addCol(cols[0] + "-UV��MID��", cols[3]);
            String[] _cols = ipvMap.get(cols[0]);
            if (_cols != null) {
                detail.addCol(cols[0] + "-IPV", _cols[0]);
                detail.addCol(cols[0] + "-IPV-UV����Ա��", _cols[1]);
                detail.addCol(cols[0] + "-IPV-UV��MID��", _cols[2]);
                detail.addCol(cols[0] + "���ת����(��ֱ�)", Fmt.parent3(_cols[1], cols[2]));

                total.addCol(cols[0]).addCol(cols[1]).addCol(cols[2])
                        /*.addCol(cols[3])*/.addCol(_cols[0]).addCol(_cols[1])
                        /* .addCol(_cols[2])*/.addCol(Fmt.parent2(_cols[1], cols[2])).addCol(Fmt.div(cols[1], cols[3]))
                        .addCol(Report.BREAK_VALUE);

                String[] _tradeCols = tradeMap.get(cols[0]);
                if (_tradeCols != null) {
                    detail.addCol(cols[0] + "-֧�������", _tradeCols[0]);
                    detail.addCol(cols[0] + "-����UV", _tradeCols[2]);
                    detail.addCol(cols[0] + "-������", _tradeCols[4]);
                    detail.addCol(cols[0] + "����ת����(��ֱ�)", Fmt.parent3(_tradeCols[2], _cols[1]));

                    trade.addCol(cols[0]).addCol(_tradeCols[0]).addCol(_cols[1])
                            /*.addCol(cols[3])*/.addCol(_tradeCols[2]).addCol(Fmt.parent2(_tradeCols[2], _cols[1]))
                            /* .addCol(_cols[2])*/.addCol(_tradeCols[4]).addCol(Fmt.div(_tradeCols[0], _tradeCols[2]))
                            .addCol(Report.BREAK_VALUE);
                }
            }


        }


        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
    }

    private static List<String> HOSTS = Arrays.asList(new String[]{
            "search.taobao.com",
             "love.taobao.com",
            "s.taobao.com",
            "s.etao.com",
            "list.taobao.com",
            "s8.taobao.com",
            "search8.taobao.com"
    });

}
