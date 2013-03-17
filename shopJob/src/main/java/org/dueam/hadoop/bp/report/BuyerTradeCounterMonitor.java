package org.dueam.hadoop.bp.report;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.dueam.hadoop.common.util.Fmt;
import org.dueam.hadoop.common.util.MapUtils;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.hadoop.services.Category;
import org.dueam.hadoop.utils.uic.UicUtils;
import org.dueam.hadoop.utils.uic.UserDO;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * User: windonly
 * Date: 10-12-20 下午6:08
 */
public class BuyerTradeCounterMonitor {
    public static void main(String[] args) throws Exception {
        String input = args[0];
        if (!new File(input).exists()) {
            return;
        }
        Report report = Report.newReport("大买家监控");
        Table table = report.newViewTable("total", "最近90天购买数超过2W(父订单)的买家列表");
        table.addCol(null, "USER_ID");
        table.addCol(null, "昵称");
        table.addCol(null, "状态");
        table.addCol(null, "总订单数");
        table.addCol(null, "待付款");
        table.addCol(null, "待确认收货");
        table.addCol(null, "待评价");
        table.addCol(Report.newBreakValue());
        for (String line : Utils.read(input)) {
            String[] _cols = StringUtils.splitPreserveAllTokens(line, Utils.TAB);
            UserDO user = UicUtils.getUser(_cols[0]);
            //System.out.println(user);
            table.addCol(null, _cols[0],_cols[0]);
            table.addCol(null, user.getNick(), user.getNick());
            table.addCol(null, null, user.getSuspendedStatus());
            table.addCol(null, "总订单数", _cols[1]);
            table.addCol(null, "待付款", _cols[2]);
            table.addCol(null, "待确认收货", _cols[3]);
            table.addCol(null, "待评价", _cols[4]);
            table.addCol(Report.newBreakValue());
        }


        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));
    }


}