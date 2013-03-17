package org.dueam.hadoop.common.util;

import static org.apache.commons.lang.math.NumberUtils.toLong;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;

/**
 * User: windonly
 * Date: 10-12-21 下午2:41
 */
public class Fmt {
    /**
     * 格式化金额 (忽略分)
     * 
     * @param cent
     *            单位：分
     */
    public static String money(long cent) {
        if (cent < 1)
            return "-";
        long yuan = cent / 100;
        //亿
        if (yuan >= 100000000) {
            return div(yuan, 100000000) + "亿";
        } else if (yuan >= 10000) {
            return div(yuan, 10000) + "万";
        } else {
            return yuan + "";
        }
    }

    public static String moneyFmt(String yuanStr) {
        long cent = NumberUtils.toLong(yuanStr) * 100;
        if (cent < 1)
            return "-";
        long yuan = cent / 100;
        //亿
        if (yuan >= 100000000) {
            return div(yuan, 100000000) + "亿";
        } else if (yuan >= 10000) {
            return div(yuan, 10000) + "万";
        } else {
            return yuan + "";
        }
    }

    /**
     * 整除保留两位小数
     * 
     * @param value
     * @param div
     * @return
     */
    public static String div(long value, long div) {
        if (div == 0) {
            return "-";
        }
        if (value % div == 0) {
            return String.valueOf(value / div);
        }
        return String.valueOf((double) ((value * 100) / div) / 100);
    }

    public static String div(String valueStr, String divStr) {
        long value = NumberUtils.toLong(valueStr);
        long div = NumberUtils.toLong(divStr);
        if (div == 0) {
            return "-";
        }
        if (value % div == 0) {
            return String.valueOf(value / div);
        }
        return String.valueOf((double) ((value * 100) / div) / 100);
    }

    /**
     * return int value
     * 
     * @param valueStr
     * @param divStr
     * @return
     */
    public static String divFixed(String valueStr, String divStr) {
        long value = NumberUtils.toLong(valueStr);
        long div = NumberUtils.toLong(divStr);
        if (div == 0) {
            return "-";
        }
        if (value % div == 0) {
            return String.valueOf(value / div);
        }
        return String.valueOf(((value * 100) / div) / 100);
    }

    public static String fixed(String valueStr) {
        if (StringUtils.indexOf(valueStr, '.') > 0) {
            return StringUtils.substring(valueStr, 0, StringUtils.indexOf(valueStr, '.') + 4);
        }
        return valueStr;
    }

    public static String divNumber(long value, long div) {
        if (div == 0) {
            return "-";
        }
        return String.valueOf(value / div);
    }

    /**
     * 格式化金额
     * 
     * @param money
     *            单位：元
     */
    public static String money(String money) {
        return number(String.valueOf(money));
    }

    /**
     * 格式化金额
     * 
     * @param cent
     *            单位：分
     */
    public static String money(int cent) {
        return number(String.valueOf((double) cent / 100));
    }

    /**
     * 格式化数字 12345.23 => 12,345.23 123456 => 123,456
     * 
     * @param num
     * @return
     */
    public static String number(long num) {
        return number(String.valueOf(num));
    }

    /**
     * 格式化数字 12345.23 => 12,345.23 123456 => 123,456
     * 
     * @param num
     * @return
     */
    public static String number(int num) {
        return number(String.valueOf(num));
    }

    /**
     * 格式化数字 12345.23 => 12,345.23 123456 => 123,456
     * 
     * @param num
     * @return
     */
    public static String number(double num) {
        return number(String.valueOf(num));
    }

    /**
     * 格式化数字 12345.23 => 12,345.23 123456 => 123,456
     * 
     * @param number
     * @return
     */
    public static String number(String number) {
        StringBuffer sb = new StringBuffer();
        int max = number.length();
        if (number.indexOf('.') >= 0) {
            max = number.indexOf('.');
        }
        int dot = 3 - max % 3;
        for (int i = 0; i < max; i++) {
            sb.append(number.charAt(i));
            dot++;
            if (dot == 3) {
                sb.append(',');
                dot = 0;
            }
        }
        if (sb.charAt(sb.length() - 1) == ',') {
            sb.deleteCharAt(sb.length() - 1);
        }
        if (max < number.length()) {
            sb.append(number.substring(max, number.length()));
        }
        return sb.toString();

    }

    public static String growing(long now, long last) {
        if (now == last || now == 0 || last == 0)
            return "-";
        double parent = ((double) ((now - last) * 10000 / last)) / 100;
        if (Math.abs(parent) < 0.1) {
            return "-";
        }
        if (now > last) {
            return "<span style='color:red'>↑ " + parent + "%</span>";
        }
        return "<span style='color:green'>↓ " + (-parent) + "%</span>";
    }

    public static String parent(long curr, long sum) {
        if (curr == 0 || sum == 0)
            return "-";
        double parent = ((double) ((curr) * 10000 / sum)) / 100;
        if (parent < 0.01) {
            return "-";
        }
        return parent + "%";
    }

    public static String parent2(String currStr, String sumStr) {
        long curr = toLong(currStr);
        long sum = toLong(sumStr);
        if (curr == 0 || sum == 0)
            return "-";
        double parent = ((double) ((curr) * 1000000 / sum)) / 10000;
        if (parent < 0.01) {
            return "-";
        }
        return parent + "%";
    }

    public static String parent3(String currStr, String sumStr) {
        long curr = toLong(currStr);
        long sum = toLong(sumStr);
        if (curr == 0 || sum == 0)
            return "-";
        double parent = ((double) ((curr) * 10000 / sum));
        if (parent < 1) {
            return "-";
        }
        return String.valueOf((long) parent);
    }

    public static String parent4(String currStr, String sumStr) {
        long curr = toLong(currStr);
        long sum = toLong(sumStr);
        if (curr == 0 || sum == 0)
            return "-";
        return String.valueOf(curr / sum);
    }

    public static String parent5(String currStr, String sumStr) {

        long curr = toLong(currStr);
        long sum = toLong(sumStr);

        if (curr == 0 || sum == 0)
            return "-";
        double parent = ((double) ((curr) * 10000 / sum)) / 100;
        if (parent < 0.01) {
            return "-";
        }
        return String.valueOf(parent);
    }

    public static String milli(String currStr, String sumStr) {
        long curr = NumberUtils.toLong(currStr, 0);
        long sum = NumberUtils.toLong(sumStr, 0);
        if (curr == 0 || sum == 0)
            return "0";
        long m = curr * 1000 / sum;

        return String.valueOf(m);
    }

    public static String parent(double curr, double sum) {
        if (curr == 0 || sum == 0)
            return "-";
        double parent = ((double) ((curr) * 10000 / sum)) / 100;
        if (parent < 0.01) {
            return "-";
        }
        return parent + "%";
    }

    public static long parseLong(String str) {
        if (StringUtils.isNotBlank(str)) {
            try {
                return (long) Double.parseDouble(str);
            } catch (NumberFormatException e) {
                return 0;
            }

        } else {
            return 0;
        }

    }
}
