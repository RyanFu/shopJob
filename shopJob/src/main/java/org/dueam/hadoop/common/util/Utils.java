package org.dueam.hadoop.common.util;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.dueam.hadoop.common.tables.TcBizOrder;

import javax.security.auth.callback.Callback;
import java.io.*;
import java.util.*;

/**
 * my string utils
 * User: windonly
 * Date: 10-12-16 下午5:33
 */
public abstract class Utils {
    public static String asString(String... args) {
        StringBuffer _tmp = new StringBuffer();
        for (String value : args) {
            _tmp.append(value).append(',');
        }
        if (_tmp.length() > 0) {
            _tmp.deleteCharAt(_tmp.length() - 1);
        }
        return _tmp.toString();
    }

    public static String getHost(String input) {
        if (input == null) return null;
        String url = StringUtils.trim(input.toLowerCase());
        if (url.startsWith("http")) {
            if (url.length() < 7) return null;
            url = url.substring(7);
        } else if (url.startsWith("https")) {
            if (url.length() < 8) return null;
            url = url.substring(8);
        }
        if (url.indexOf('/') >= 0) {
            return url.substring(0, url.indexOf('/'));
        }
        return url;
    }

    public static boolean isNull(String input) {
        return StringUtils.isEmpty(input) || StringUtils.equalsIgnoreCase(input, "\\N") || StringUtils.equalsIgnoreCase(input, "null");
    }

    public static boolean isNotNull(String input) {
        return !isNull(input);
    }

    public static String[] asArray(String line) {
        if (null == line) return new String[0];
        return line.split(",");
    }

    public static String getValue(String attr, String key, String defaultValue) {
        String rootCat = defaultValue;
        attr = ';' + attr;
        if (attr != null && attr.indexOf(';' + key + ':') >= 0) {
            rootCat = attr.substring(attr.indexOf(';' + key + ':') + key.length() + 2);
            if (rootCat.indexOf(';') >= 0) {
                rootCat = rootCat.substring(0, rootCat.indexOf(';'));
            }
        }
        return rootCat;
    }

    /**
     * 判断两个时间是否在同一天
     *
     * @param date
     * @param otherDate
     * @return
     */
    public static boolean isSameDay(String date, String otherDate) {
        if (date == null && otherDate == null) return true;
        if (date == null || otherDate == null) return false;
        if (otherDate.length() < 10) return false;
        return date.indexOf(otherDate.substring(0, 10)) >= 0;
    }

    /**
     * if fetch one of days
     *
     * @param query query date
     * @param dates days
     * @return
     */
    public static boolean isSameDayFetchOne(String query, String... dates) {
        for (String date : dates) {
            if (isSameDay(date, query)) return true;
        }
        return false;
    }

    public final static char TAB = 0x09;

    /**
     * merge key with tab
     *
     * @param keys
     * @return
     */
    public static Text mergeKey(String... keys) {
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < keys.length; i++) {
            if (i > 0) {
                sb.append(TAB);
            }
            sb.append(keys[i]);
        }
        return new Text(sb.toString());
    }

    public static Text mergeKey(String[] keys, String[] keys2) {
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < keys.length; i++) {
            if (i > 0) {
                sb.append(TAB);
            }
            sb.append(keys[i]);
        }
        for (int i = 0; i < keys2.length; i++) {
            sb.append(TAB);
            sb.append(keys2[i]);
        }
        return new Text(sb.toString());
    }

    /**
     * merge key with tab
     *
     * @param keys
     * @return
     */
    public static String merge(String... keys) {
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < keys.length; i++) {
            if (i > 0) {
                sb.append(TAB);
            }
            sb.append(keys[i]);
        }
        return sb.toString();
    }

    public static LongWritable one() {
        return new LongWritable(1);
    }


    public static void main(String[] args) {
        System.out.println(getSubDomain("lining.taobao.com"));
        System.out.println(getSubDomain("lining"));
        System.out.println(getSubDomain("lining."));
        System.out.println(isSameDay("2010-10-10 18:23:12", "2010-10-10 08:23:12"));
        System.out.println(isSameDay("2010-10-10 18:23:12", "2010-10-09"));
        System.out.println(getValue("key1:value1;key2:value2;key3:value3;", "key1", ""));
        System.out.println(getValue(";key1:value1;key2:value2;key3:value3", "key1", ""));
    }

    public static String[] subArray(String[] array, int startIndex) {
        if (array == null) {
            return null;
        }
        if (startIndex < 0) {
            startIndex = 0;
        }
        int newSize = array.length - startIndex;
        if (newSize <= 0) {
            return new String[0];
        }

        String[] subArray = new String[newSize];
        System.arraycopy(array, startIndex, subArray, 0, newSize);
        return subArray;
    }

    public static String[] subArray(String[] array, int startIndex, int endIndex) {
        if (array == null) {
            return null;
        }
        if (startIndex < 0) {
            startIndex = 0;
        }
        int newSize = endIndex - startIndex;
        if (newSize <= 0) {
            return new String[0];
        }

        String[] subArray = new String[newSize];
        System.arraycopy(array, startIndex, subArray, 0, newSize);
        return subArray;
    }

    /**
     * 汇总数
     *
     * @param values
     * @param countArea [0,10,100]
     * @return
     */
    public static long[] count(long[] values, long[] countArea) {
        long[] result = new long[countArea.length];
        for (long value : values)
            for (int i = countArea.length - 1; i >= 0; i--) {
                if (value >= countArea[i]) {
                    result[i]++;
                    break;
                }
            }
        return result;
    }

    public static long sum(long[] values) {
        long sum = 0;
        for (long value : values) {
            sum += value;
        }
        return sum;
    }

    public static List<String> read(String input) throws IOException {
        File file = new File(input);
        if (!file.exists()) return new ArrayList<String>(0);
        return FileUtils.readLines(file);
    }

    public static List<String> readWithCharset(String input, String charset) throws IOException {
        File file = new File(input);
        if (!file.exists()) return new ArrayList<String>(0);
        return FileUtils.readLines(file, charset);
    }

    public static List<String> read(String input, String[] exclude) throws IOException {
        File file = new File(input);
        if (!file.exists()) return new ArrayList<String>(0);
        InputStream in = null;
        try {
            in = FileUtils.openInputStream(file);
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            List list = new ArrayList();
            String line = reader.readLine();
            while (line != null) {
                boolean skip = false;
                if (exclude != null) {
                    for (String ex : exclude) {
                        if (line.startsWith(ex)) {
                            skip = true;
                            break;
                        }
                    }
                }
                if (!skip)
                    list.add(line);
                line = reader.readLine();
            }
            return list;
        } finally {
            IOUtils.closeQuietly(in);
        }

    }

    public static List<String> readInclude(String input, String[] include) throws IOException {
        File file = new File(input);
        if (!file.exists()) return new ArrayList<String>(0);
        InputStream in = null;
        try {
            in = FileUtils.openInputStream(file);
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            List list = new ArrayList();
            String line = reader.readLine();
            while (line != null) {
                boolean skip = false;
                if (include != null) {
                    skip = true;
                    for (String ex : include) {
                        if (line.startsWith(ex)) {
                            skip = false;
                            break;
                        }
                    }
                }
                if (!skip)
                    list.add(line);
                line = reader.readLine();
            }
            return list;
        } finally {
            IOUtils.closeQuietly(in);
        }

    }

    public static String getMax(String input, String key, int compRow, char splitChar) throws IOException {
        File file = new File(input);
        if (!file.exists()) return null;
        InputStream in = null;
        try {
            in = FileUtils.openInputStream(file);
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            String line = reader.readLine();
            long max = 0;
            String maxLine = null;
            while (line != null) {
                if (StringUtils.startsWith(line, key)) {
                    String[] _cols = StringUtils.splitPreserveAllTokens(line, splitChar);
                    if (_cols.length > compRow) {
                        long value = NumberUtils.toLong(_cols[compRow], 0);
                        if (value > max) {
                            max = value;
                            maxLine = line;
                        }
                    }
                }

                line = reader.readLine();
            }
            return maxLine;
        } finally {
            IOUtils.closeQuietly(in);
        }

    }

    public interface Callback {
        public void call(String line);
    }

    public static void viewLines(String input, Callback callback) throws IOException {
        File file = new File(input);
        if (!file.exists()) return;
        InputStream in = null;
        try {
            in = FileUtils.openInputStream(file);
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            String line = reader.readLine();
            while (line != null) {
                callback.call(line);
                line = reader.readLine();
            }
        } finally {
            IOUtils.closeQuietly(in);
        }

    }

    public static Map<String, String> asMap(String... keyValues) {
        Map<String, String> map = new HashMap();
        for (int i = 0; i < keyValues.length; i = i + 2) {
            map.put(keyValues[i], keyValues[i + 1]);

        }
        return map;
    }

    public static Map<String, String> asLinkedMap(String... keyValues) {
        Map<String, String> map = new LinkedHashMap();
        for (int i = 0; i < keyValues.length; i = i + 2) {
            map.put(keyValues[i], keyValues[i + 1]);
        }
        return map;
    }

    static long[] twoPow = new long[]{1L, 2L, 4L, 8L, 16L, 32L, 64L, 128L, 256L, 512L, 1024L, 2048L, 4096L, 8192L, 16384L, 32768L, 65536L, 131072L, 262144L, 524288L, 1048576L, 2097152L, 4194304L, 8388608L, 16777216L, 33554432L, 67108864L, 134217728L, 268435456L, 536870912L, 1073741824L, 2147483648L, 4294967296L, 8589934592L, 17179869184L, 34359738368L, 68719476736L, 137438953472L, 274877906944L, 549755813888L, 1099511627776L, 2199023255552L, 4398046511104L, 8796093022208L, 17592186044416L, 35184372088832L, 70368744177664L, 140737488355328L, 281474976710656L, 562949953421312L, 1125899906842624L, 2251799813685248L, 4503599627370496L, 9007199254740992L, 18014398509481984L, 36028797018963968L, 72057594037927936L, 144115188075855872L, 288230376151711744L, 576460752303423488L, 1152921504606846976L, 2305843009213693952L, 4611686018427387904L};

    public static List<Long> toBitArray(long value) {
        List result = new ArrayList<Long>();
        for (long pow : twoPow) {
            if (pow > value) {
                break;
            }
            if ((value & pow) > 0) {
                result.add(pow);
            }
        }
        return result;
    }

    /**
     * 将分转成元（去除小数）
     *
     * @param fen
     * @return
     */
    public static String toYuan(String fen) {
        return String.valueOf(NumberUtils.toLong(fen, 0) / 100);
    }

    public static String getSubDomain(String host) {
        if (StringUtils.indexOf(host, '.') >= 0) {
            return StringUtils.substring(host, 0, StringUtils.indexOf(host, '.'));
        }
        return host;
    }


}
