package org.dueam.hadoop.bp.report;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.dueam.hadoop.common.util.Fmt;
import org.dueam.hadoop.common.util.MapUtils;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

/**
 * list�������
 */
public class ListAllReport {
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

        Report report = Report.newReport(name + "list���");

        Map<String, List<String[]>> today = MapUtils.map(Utils.read(input), CTRL_A);

        // �������ݣ���PV��UV��UUV��MUV���˾�PV������ת���ʣ�����ת���ʣ��˾�PV����ֲ���1��2-5��6-10��10-20��20-50��50-��

        Table total = report.newTable("total", "��������");

        List<String[]> ls = today.get("ls");
        List<String[]> ipv = today.get("ipv");
        List<String[]> trade = today.get("trade");
        List<String[]> avgPV = today.get("avg_pv");

        total.addCol("PV", ls.get(0)[0]);
        total.addCol("UV", ls.get(0)[1]);
        total.addCol("UUV", ls.get(0)[2]);
        total.addCol("MUV", ls.get(0)[3]);

        total.addCol("�˾�PV", Fmt.div(ls.get(0)[0], ls.get(0)[1]));
        total.addCol("���ת����(�ٷֱ�)", Fmt.parent5(ipv.get(0)[1], ls.get(0)[1]));
        total.addCol("����ת����(�ٷֱ�)", Fmt.parent5(trade.get(0)[2], ipv.get(0)[1]));

        Table avg = report.newGroupTable("avg", "�˾�PV����ֲ�");

        for (String[] s : avgPV) {
            avg.addCol(s[0], s[1]);
        }

        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));

    }

}
