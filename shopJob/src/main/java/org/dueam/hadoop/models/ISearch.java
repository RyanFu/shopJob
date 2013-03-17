package org.dueam.hadoop.models;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.dueam.hadoop.common.tables.ISearchLog;


/**
 * User: windonly
 * Date: 11-4-11 ÏÂÎç3:22
 */
public class ISearch {
    static String input = "INFO\t47840804055344\t03-29 00:00:00\t17\t699\t716\tpromoted_status=1&username=667623510&filter=starts:sysdate+60~&filter=approve_status:0,1,-9&nk=y&s=0&n=1&mcn=ic067130.cm3&src=c2c-ic_instant-172.23.67.130";

    public static void main(String[] args) {
        String[] _allCols = StringUtils.splitPreserveAllTokens(input, ISearchLog.SPLIT_CHAR);
        long runTime = NumberUtils.toLong(_allCols[ISearchLog.RUN_TIME]);
        long queryTime = NumberUtils.toLong(_allCols[ISearchLog.QUERY_TIME]);
        for (String kv : StringUtils.splitPreserveAllTokens(_allCols[ISearchLog.QUERY_URL], '&')) {
            String[] array = StringUtils.splitPreserveAllTokens(kv, '=');
            if (array.length < 2) continue;
            String key = array[0] ;
            String value = array[1];
            if("filter".equals(key)){
                String[] _array = StringUtils.splitPreserveAllTokens(value, ':');

                //_array[0]
            }else if("src".equals(key)){
                  String[] _array = StringUtils.splitPreserveAllTokens(value, '-');
            }

        }

    }

    final static char TAB = 0x09;
}
