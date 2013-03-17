package org.dueam.hadoop.common.util;

import java.text.ParseException;
import java.util.Date;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.commons.lang.time.DateUtils;
import org.dueam.hadoop.common.tables.AuctionAuctions;

public abstract class ItemUtils {
    private final static String[] ALL_PARA = new String[]{"item_id",
            "itemId", "item_num_id", "itemNumId", "taomi", "id"};
    final static String[] ITEM_OLD_ID = new String[]{"item_detail-0db2-",
            "item_detail-0db1-", "item_detail-db2-", "item_detail-db2-",
            "item_detail-null-"};

    public static String getId(String url) {
        if (url == null)
            return null;
        if (url.toLowerCase().startsWith("http://item.taobao.com") || url.toLowerCase().startsWith("http://item.tmall.com") || url.toLowerCase().startsWith("http://item.mall.taobao.com")) {

            if (url.indexOf("item_detail.") >= 0 || url.indexOf("itemDetail.") >= 0
                    || url.indexOf("/item.htm") >= 0) {
                String[] _array = StringUtils.split(url, "?&");
                for (String para : _array) {
                    if (para.indexOf('=') >= 0) {
                        String[] _a = StringUtils.split(para, "=");
                        if (_a != null && _a.length > 1
                                && ArrayUtils.indexOf(ALL_PARA, _a[0], 0) != -1) {
                            return _a[1];
                        }
                    }
                }
            } else if (url.indexOf("/item_detail-") >= 0) {
                return getOldItemId(url);
            }
        }

        return null;
    }

    private static String getOldItemId(String url) {
        if (url == null)
            return null;
        String id = null;
        for (String para : ITEM_OLD_ID) {
            if (url.lastIndexOf(para) >= 0) {
                String _tmp = url.substring(url.lastIndexOf(para)
                        + para.length());
                if (_tmp.indexOf(".jhtml") > 0) {
                    id = _tmp.substring(0, _tmp.indexOf(".jhtml"));
                } else if (_tmp.indexOf(".htm") > 0) {
                    id = _tmp.substring(0, _tmp.indexOf(".htm"));
                } else {
                    id = _tmp;
                }
            }
        }
        if (null == id)
            return null;
        return id.length() == 32 ? id : null;

    }

    /**
     * 返回商品状态(在售,仓库中,删除)
     *
     * @param _allCols
     * @return
     */
    public static String getStatus(String[] _allCols, String now) {
        String status = _allCols[AuctionAuctions.AUCTION_STATUS];
        if ("0,1,-9".indexOf(status) >= 0) {
            String userType = _allCols[AuctionAuctions.USER_TYPE];
            if ("1".equals(userType)) {
                return ONLINE;
            }
            String start = _allCols[AuctionAuctions.STARTS];
            int days = NumberUtils.toInt(_allCols[AuctionAuctions.DURATION])
                    * NumberUtils.toInt(_allCols[AuctionAuctions.REPOST_COUNT])
                    - 1;
            try {
                Date sysdate = DateUtils.parseDate(now,
                        new String[]{"yyyy-MM-dd"});
                String end = DateFormatUtils.format(DateUtils.addDays(sysdate,
                        -days), "yyyy-MM-dd");
                return end.compareTo(start) > 0 ? OFFLINE : ONLINE;

            } catch (ParseException e) {
            }

        } else if ("-1,-4".indexOf(status) >= 0) {
            return DEL;
        } else {
            return OFFLINE;
        }
        return null;
    }

    public static String getItemStatusName(String status) {
        if ("0".equals(status)) return "在售";
        if ("1".equals(status)) return "在售(小二确认)";
        if ("-1".equals(status)) return "用户删除";
        if ("-2".equals(status)) return "用户下架";
        if ("-3".equals(status)) return "小二下架";
        if ("-4".equals(status)) return "小二删除";
        if ("-5".equals(status)) return "从未上架";
        if ("-9".equals(status)) return "CC";
        return "NONE";
    }

    public static String[] allStatus = new String[]{"0", "1", "-1", "-2", "-3", "-4", "-5", "-9"};

    public static String ONLINE = "ONLINE";
    public static String OFFLINE = "OFFLINE";
    public static String DEL = "DEL";

    public static void main(String[] args) throws Exception {

        Date sysdate = DateUtils.parseDate("2008-10-13",
                new String[]{"yyyy-MM-dd"});
        String end = DateFormatUtils.format(DateUtils.addDays(sysdate,
                -7), "yyyy-MM-dd");
        System.out.println(end);
        System.out.println(getHost("HTTP://WWW.TO.CM/I.HTM"));
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


}
