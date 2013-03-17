package org.dueam.hadoop.bp.report;

import org.apache.commons.lang.math.NumberUtils;
import org.dueam.hadoop.common.util.ItemUtils;
import org.dueam.hadoop.common.util.MapUtils;
import org.dueam.hadoop.common.util.Utils;
import org.dueam.report.common.Report;
import org.dueam.report.common.Table;
import org.dueam.report.common.XmlReportFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.dueam.hadoop.common.Functions.str;

/**
 * User: windonly
 * Date: 11-1-6 下午5:24
 */
public class UserCenter {
    public static void main(String[] args) throws IOException {
        String input = args[0];
        if (!new File(input).exists()) {
            return;
        }
        Report report = Report.newReport("用户中心报表");
        Map<String, List<String[]>> today = MapUtils.map(Utils.read(input, null));

        if (today.get("total") != null) {
            Table table = report.newTable("total", "用户基本信息");
            long sum = 0;
            for (String[] array : today.get("total")) {
                table.addCol(array[0], get(namesMap, array[0]), array[1]);
                sum += NumberUtils.toLong(array[1]);
            }
            table.addCol("sum", "合计", str(sum));
        }

        if (today.get("suspended") != null) {
            Table table = report.newGroupTable("suspended", "今天注册用户状态分布");
            for (String[] array : today.get("suspended")) {
                table.addCol(array[0], get(suspendedMap, array[0]), array[1]);
            }
        }

        if (today.get("user_active") != null) {
            Table table = report.newGroupTable("user_active", "今天激活用户激活方式分布");
            for (String[] array : today.get("user_active")) {
                table.addCol(array[0], get(activeMap, array[0]), array[1]);
            }
        }



        XmlReportFactory.dump(report, new FileOutputStream(args[0] + ".xml"));

    }


    public static String get(Map<String, String> map, String key) {
        String value = map.get(key);
        if (value != null) return value;
        return key;
    }

    static Map<String, String> namesMap = Utils.asMap("reg,注册用户,active,激活用户".split(","));
    static Map<String, String> suspendedMap = Utils.asMap("0,正常,1,未激活,2,删除,3,冻结,-9,CC".split(","));
    static Map<String, String> activeMap = Utils.asMap("1,邮件,2,手机,3,手机、邮件,4,输入邮件上的校验码,7,免激活,8,快速注册,11,手机注册,12,wap 注册的会员,13,淘宝机票注册,14,注册小二账号,15,wap 短信注册,16,微支付自动注册,17,新版支付宝快速Q（免激活）".split(","));
}
