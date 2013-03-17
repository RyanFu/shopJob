package org.dueam.hadoop.utils.uic;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.RandomUtils;
import org.dueam.hadoop.utils.HttpUtils;
import org.dueam.services.ServiceEnv;

public class UicUtils {

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
//        List<String> lines =
//                FileUtils.readLines(new File("user"), "gbk");
//        List<String> result = new ArrayList<String>();
//        for (String nick : lines) {
//            UserDO user = UicUtils.getUser(nick, 3);
//            result.add(nick + "\t" + user.getId() + "\t" + user.getBuyerRate() + "\t" + user.getSellerRate());
//        }
//        FileUtils.writeLines(new File("user.uic"), "GBK", result);
          System.out.println(getUser("820846"));
    }

    static Map<String, UserDO> cache = new HashMap<String, UserDO>();
    static boolean OPEN_CACHE = false;

    public static void openCache() {
        OPEN_CACHE = true;
    }

    static UserDO lastUser = null;
    static int pos = 0;

    public static UserDO getUser(String userId) {

        if (OPEN_CACHE) {
            if (cache.containsKey(userId)) {
                return cache.get(userId);
            }
        }
        if (lastUser != null && StringUtils.equals(lastUser.getId(), userId)) {
            return lastUser;
        }
        UserDO user = new UserDO();
        if(true){
            String content = HttpUtils.getContent(ServiceEnv.SERVICE_USER_ID+"/"+userId, "utf-8");
            user.setId(getCol2(content, "userId"));
            user.setNick(getCol2(content, "nick"));
            user.setSellerRate(getCol2(content, "rateSum"));
            user.setSuspended(getCol2(content, "suspended"));
        }
        if (false) {
            String url = UIC_CONSOLE_CACHE + UIC_IP[pos] + "&userId=" + userId;
            pos++;
            if (pos >= UIC_IP.length) {
                pos = 0;
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            String content = HttpUtils.getContent(url, "utf-8");
            if (content == null || !content.contains("userId=")) {
                content = HttpUtils.getContent(UIC_CONSOLE + userId, "utf-8");
            }
            user.setId(getCol(content, "userId"));
            user.setNick(getCol(content, "nick"));
            user.setSellerRate(getCol(content, "rateSum"));
            user.setSuspended(getCol(content, "suspended"));
        }
        if (OPEN_CACHE) {
            cache.put(userId, user);
        }
        lastUser = user;

        return user;
    }

    public static int NUM_NICK = 3;
    public static int NUM_USER_ID = 2;

    public static UserDO getUser(String userId, int /* 2=user_id 3=nikc */ num) {

        if (OPEN_CACHE) {
            if (cache.containsKey(userId)) {
                return cache.get(userId);
            }
        }
        if (lastUser != null && StringUtils.equals(lastUser.getId(), userId)) {
            return lastUser;
        }
        UserDO user = new UserDO();
        String url = UIC_CONSOLE_CACHE + "?num=" + num + "&requestIp=" + UIC_IP[pos] + "&userId=" + userId;
        pos++;
        if (pos >= UIC_IP.length) {
            pos = 0;
        }
        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        String content = HttpUtils.getContent(url, "utf-8");
        if (content == null || !content.contains("userId=")) {
            System.out.println(url);
            content = HttpUtils.getContent(UIC_CONSOLE + userId, "utf-8");
        }
        user.setId(getCol(content, "userId"));
        user.setNick(getCol(content, "nick"));
        user.setSellerRate(getCol(content, "rateSum"));
        user.setSuspended(getCol(content, "suspended"));
        user.setBuyerRate(getCol(content, "rateNum"));

        if (OPEN_CACHE) {
            cache.put(userId, user);
        }
        lastUser = user;

        return user;
    }

    public static String UIC_CONSOLE_CACHE_URL = "http://110.75.12.65:9999/uicConsole/viewCacheUserInfo.do";
    public static String UIC_CONSOLE = "http://172.23.13.53:7001/uicConsole//queryMysql.do?num=2&permission=low&userId=";
    public static String UIC_CONSOLE_CACHE = "http://172.23.13.53:7001/uicConsole/viewCacheUserInfo.do?num=2&requestIp=";

    // private static String UIC_CONSOLE =
    // "http://110.75.12.65/uicConsole//queryOracle.do?num=2&dbs=1&userId=";
    private static String getCol(String text, String name) {
        for (String col : text.split(",")) {
            if (col.indexOf(name + "=") == 0) {
                String[] _array = col.split("=");
                if (_array.length > 1) {
                    return col.split("=")[1];
                } else {
                    return null;
                }
            }
        }
        return null;
    }
    private static String getCol2(String text, String name) {
        for (String col : text.split(",")) {
            col = StringUtils.remove(col,"\"");
            if (col.indexOf(name + ":") == 0) {
                String[] _array = col.split(":");
                if (_array.length > 1) {
                    return col.split(":")[1];
                } else {
                    return null;
                }
            }
        }
        return null;
    }

    public static String[] allRate = {"ÎÞÐÅÓÃ", "1ÐÄ", "2ÐÄ", "3ÐÄ", "4ÐÄ",
            "5ÐÄ", "1×ê", "2×ê", "3×ê", "4×ê", "5×ê", "1»Ê¹Ú", "2»Ê¹Ú", "3»Ê¹Ú", "4»Ê¹Ú",
            "5»Ê¹Ú", "1½ð¹Ú", "2½ð¹Ú", "3½ð¹Ú", "4½ð¹Ú", "5½ð¹Ú"};

    public static int getRankPos(String rank) {
        for (int i = 0; i < allRate.length; i++) {
            if (StringUtils.equals(rank, allRate[i])) {
                return i;
            }
        }
        return -1;
    }

    public static int getRankPos(long rate) {
        return getRankPos(getRate(rate));
    }

    public static String getRate(long rate) {
        if (rate < 4)
            return "ÎÞÐÅÓÃ";
        else if (rate >= 4 && rate <= 10)
            return "1ÐÄ";
        else if (rate >= 11 && rate <= 40)
            return "2ÐÄ";
        else if (rate >= 41 && rate <= 90)
            return "3ÐÄ";
        else if (rate >= 91 && rate <= 150)
            return "4ÐÄ";
        else if (rate >= 151 && rate <= 250)
            return "5ÐÄ";
        else if (rate >= 251 && rate <= 500)
            return "1×ê";
        else if (rate >= 501 && rate <= 1000)
            return "2×ê";
        else if (rate >= 1001 && rate <= 2000)
            return "3×ê";
        else if (rate >= 2001 && rate <= 5000)
            return "4×ê";
        else if (rate >= 5001 && rate <= 10000)
            return "5×ê";
        else if (rate >= 10001 && rate <= 20000)
            return "1»Ê¹Ú";
        else if (rate >= 20001 && rate <= 50000)
            return "2»Ê¹Ú";
        else if (rate >= 50001 && rate <= 100000)
            return "3»Ê¹Ú";
        else if (rate >= 100001 && rate <= 200000)
            return "4»Ê¹Ú";
        else if (rate >= 200001 && rate <= 500000)
            return "5»Ê¹Ú";
        else if (rate >= 500001 && rate <= 1000000)
            return "1½ð¹Ú";
        else if (rate >= 1000001 && rate <= 2000000)
            return "2½ð¹Ú";
        else if (rate >= 2000001 && rate <= 5000000)
            return "3½ð¹Ú";
        else if (rate >= 5000001 && rate <= 10000000)
            return "4½ð¹Ú";
        else if (rate >= 10000001)
            return "5½ð¹Ú";
        return "";
    }


    private static String[] UIC_IP = {"172.24.19.129"};
    private static String[] UIC_IP2 =
            ("172.23.13.51\n" +
                    "172.23.13.52\n" +
                    "172.23.13.54\n" +
                    "172.23.13.55\n" +
                    "172.23.13.56\n" +
                    "172.23.13.57\n" +
                    "172.23.13.58\n" +
                    "172.23.13.59\n" +
                    "172.23.13.63\n" +
                    "172.23.13.64\n" +
                    "172.23.13.65\n" +
                    "172.23.13.66\n" +
                    "172.23.13.67\n" +
                    "172.23.13.68\n" +
                    "172.23.13.69\n" +
                    "172.23.13.70\n" +
                    "172.23.13.71\n" +
                    "172.23.13.75\n" +
                    "172.23.13.76\n" +
                    "172.23.13.77\n" +
                    "172.23.13.78\n" +
                    "172.23.13.79\n" +
                    "172.23.13.80\n" +
                    "172.23.13.81\n" +
                    "172.23.3.31\n" +
                    "172.23.3.32\n" +
                    "172.23.3.33\n" +
                    "172.23.3.34\n" +
                    "172.23.3.35\n" +
                    "172.23.3.36\n" +
                    "172.23.3.37\n" +
                    "172.23.3.38\n" +
                    "172.23.3.39\n" +
                    "172.24.19.83\n" +
                    "172.24.19.84\n" +
                    "172.24.19.85\n" +
                    "172.24.19.86\n" +
                    "172.24.19.87\n" +
                    "172.24.19.88\n" +
                    "172.24.19.89\n" +
                    "172.24.19.90\n" +
                    "172.24.19.101\n" +
                    "172.24.19.102\n" +
                    "172.24.19.103\n" +
                    "172.24.19.104\n" +
                    "172.24.19.105\n" +
                    "172.24.19.106\n" +
                    "172.24.19.107\n" +
                    "172.24.19.108\n" +
                    "172.24.19.109\n" +
                    "172.24.19.110\n" +
                    "172.24.19.111\n" +
                    "172.24.19.112\n" +
                    "172.24.19.115\n" +
                    "172.24.19.116\n" +
                    "172.24.19.117\n" +
                    "172.24.19.118\n" +
                    "172.24.19.119\n" +
                    "172.24.19.120\n" +
                    "172.24.19.121\n" +
                    "172.24.19.122\n" +
                    "172.24.19.123\n" +
                    "172.24.19.124\n" +
                    "172.24.19.125\n" +
                    "172.24.19.126\n" +
                    "172.24.19.129").split("\n");

}
