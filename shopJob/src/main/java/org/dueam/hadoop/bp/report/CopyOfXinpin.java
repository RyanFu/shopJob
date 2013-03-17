package org.dueam.hadoop.bp.report;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 新品统计
 */
public class CopyOfXinpin {
    private static char CTRL_A = (char) 0x01;

    /**
     * @param args
     * @throws java.io.IOException
     */
    public static void main(String[] args) throws IOException {

        Pattern p = Pattern.compile(reg);
        Matcher m = p.matcher(s);
        while (m.find()) {
            System.out.println(m.group());
        }
    }

    private static String reg = "\\{\"CUSTOMERID.*?\"\\}";

    private static String s = "{\"CUSTOMERID\":1\"},{\"CUSTOMERID\":2\"}";

}
