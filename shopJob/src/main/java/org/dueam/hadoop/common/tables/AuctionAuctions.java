package org.dueam.hadoop.common.tables;

public class AuctionAuctions {
    public static int ID = 0;
    public static int TITLE = 1;
    public static int GMT_MODIFIED = 2;
    public static int STARTS = 3;
    public static int PICT_URL = 4;
    public static int CATEGORY = 5;
    public static int MINIMUM_BID = 6;
    public static int RESERVE_PRICE = 7;
    public static int AUCTION_TYPE = 8;
    public static int DURATION = 9;
    public static int INCREMENTNUM = 10;
    public static int CITY = 11;
    public static int PROV = 12;
    public static int SHIPPING = 13;
    public static int ENDS = 14;
    public static int CURRENT_BID = 15;
    public static int QUANTITY = 16;
    public static int HAVE_INVOICE = 17;
    public static int HAVE_GUARANTEE = 18;
    public static int STUFF_STATUS = 19;
    public static int OLD_STARTS = 20;
    public static int ZOO = 21;
    public static int PROMOTED_STATUS = 22;
    public static int SECURE_TRADE_ORDINARY_POST_FEE = 23;
    public static int SECURE_TRADE_FAST_POST_FEE = 24;
    public static int OLD_QUANTITY = 25;
    public static int POINT_PRICE = 26;
    public static int AUCTION_FLAG = 27;
    public static int OPTIONS = 28;
    public static int SECURE_TRADE_EMS_POST_FEE = 29;
    public static int AUTO_CONSIGNMENT = 30;
    public static int PROPERTY = 31;
    public static int ORDERCOST = 32;
    public static int LAST_MODIFIED = 33;
    public static int DESC_PATH = 34;
    public static int POSTAGE_ID = 35;
    public static int SHOP_CATEGORIES_ID_LISTS = 36;
    public static int USER_ID = 37;
    public static int AUCTION_ID = 38;
    public static int SPU_ID = 39;
    public static int SYNC_VERSION = 40;
    public static int AUCTION_STATUS = 41;
    public static int FEATURES = 42;
    public static int FEATURE_CC = 43;
    public static int MAIN_COLOR = 44;
    public static int AUCTION_POINT = 45;
    public static int REPOST_COUNT = 46;
    public static int PROPERTY_ALIAS = 47;
    public static int OUTER_ID = 48;
    public static int USER_TYPE = 49;
    public static int SOLD_QUANTITY = 50;
    public static int VERTICAL_MARKET = 51;

    public static boolean isAuction(String[] _allCols) {
        return "a".equals(_allCols[AUCTION_TYPE]);
    }

    public static boolean isNormal(String[] _allCols) {
        return !("-1".equals(_allCols[AUCTION_STATUS]) || "-4".equals(_allCols[AUCTION_STATUS]));
    }
}
