package org.dueam.hadoop.utils;

import java.text.ParseException;
import java.util.Date;

import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.fs.Path;

/**
 * 淘宝常用的文件路径
 *
 * @author suoni
 *
 */
public abstract class TaobaoPath {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		System.out.println(TaobaoPath.isEqualInMin("2010-10-10 10:10:10","2010-10-10 10:10:30"));
		System.out.println(newDates("20110507", 31));
		System.out.println(newDates("20110507", -31));
		System.out.println(now());

	}

	/**
	 * 获取日志路径
	 *
	 * @param date
	 * @return
	 */
	public static String log(String date) {
		if (date == null || date.length() != 8) {
			throw new IllegalArgumentException(
					"Please enter date , eg.20100508");
		}
		String year = date.substring(0, 4);
		return "/group/taobao/taobao/dw/log1/" + year + "/" + date;
	}

	public static Path cdnlog(String date) {
		if (date == null || date.length() != 8) {
			throw new IllegalArgumentException(
					"Please enter date , eg.20100508");
		}
		return new Path("/group/tbads/logdata/cdn_t2_img_log/" + date);
	}

	/**
	 * 淘宝常用的几张表
	 *
	 * @param table
	 * @param date
	 * @return
	 */
	public static String stb(String table, String date) {
		if (date == null || date.length() != 8 || table == null) {
			throw new IllegalArgumentException(
					"Please enter table and date , eg. auction_auctions,20100508");
		}
		return "/group/taobao/taobao/dw/stb/" + date + "/" + table + "/";
	}

	public static String hive(String table, String date) {
		if (date == null || date.length() != 8 || table == null) {
			throw new IllegalArgumentException(
					"Please enter table and date , eg. auction_auctions,20100508");
		}
		// return "/group/taobao/taobao/dw/stb/" + date + "/" + table + "/";

		return "/group/taobao/taobao/hive/" + table + "/pt=" + date + "000000/";
	}

	/**
	 * 获取商品表
	 *
	 * @param date
	 * @return
	 */
	public static Path stbAuctionAuctions(String date) {
		return new Path(stb("auction_auctions", date));
	}

	/**
	 * 获取商品表
	 *
	 * @param date
	 * @return
	 */
	public static Path stbBizOrder(String date) {
		return new Path(stb("tc_biz_order", date));
	}

	/**
	 * 获取商品表
	 *
	 * @param date
	 * @return
	 */
	public static Path stbCart(String date) {
		return new Path(stb("cart", date));
	}

	/**
	 * sequence 压缩 0x001分割
	 *
	 * @param date
	 * @return
	 */
	public static Path hiveAuctionAuctions(String date) {
		return new Path(hive("s_auction_auctions", date));
	}

	/**
	 * 输出目录
	 *
	 * @param name
	 * @param date
	 * @return
	 */
	// private static String SUONI_OUT_DIR = "/group/cug-tbptd-dev/suoni/";
	private static String SUONI_OUT_DIR = "/group/tbdev/xiaodu/suoni/";

	public static Path getOutput(String name, String date) {
		return new Path(SUONI_OUT_DIR + name + "/" + date + "/");
	}

	public static Path getOutput(String name) {
		return new Path(SUONI_OUT_DIR + name + "/");
	}

	public static Path getOutput(String name, String date, String subdate) {
		return new Path(SUONI_OUT_DIR + name + "/" + date + "/" + subdate + "/");
	}

	/**
	 * 增加或者减少天数
	 *
	 * @param date
	 * @param iDays
	 * @return
	 */
	public static String newDates(String date, int iDays) {
		try {
			Date now = DateUtils.parseDate(date, new String[] { "yyyyMMdd" });
			Date newDate = DateUtils.addDays(now, iDays);
			return DateFormatUtils.format(newDate, "yyyyMMdd");
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;

	}
	/**
	 * 比较时间，精确到分
	 * @param start
	 * @param end
	 * @return
	 */
	public static boolean isEqualInMin(String start, String end) {
		try {
			Date startTime = DateUtils.parseDate(start, new String[] { "yyyy-MM-dd HH:mm:ss" });
			Date endTime = DateUtils.parseDate(end, new String[] { "yyyy-MM-dd HH:mm:ss" });
			if(Math.abs(startTime.getTime() - endTime.getTime() )<10*1000){
				return true;
			}
			return false;
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return false;

	}

	public static String now() {
		try {
			Date now = new Date();
			Date newDate = DateUtils.addDays(now, -1);
			return DateFormatUtils.format(newDate, "yyyyMMdd");
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;

	}

	public static String newDates2(String date, int iDays) {
		try {
			Date now = DateUtils.parseDate(date, new String[] { "yyyy-MM-dd" });
			Date newDate = DateUtils.addDays(now, iDays);
			return DateFormatUtils.format(newDate, "yyyy-MM-dd");
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;

	}

	/**
	 * 将yyyyMMdd转化成yyyy-MM-dd
	 *
	 * @param tmp
	 * @return
	 */
	public static String dateFormat(String tmp) {
		return tmp.substring(0, 4) + "-" + tmp.substring(4, 6) + "-"
				+ tmp.substring(6, 8);
	}

	public static String getValue(String attr, String key, String defaultValue) {
		String rootCat = defaultValue;
		if (attr != null && attr.indexOf(key) >= 0) {
			rootCat = attr.substring(attr.indexOf(key) + key.length() + 1);
			if (rootCat.indexOf(';') >= 0) {
				rootCat = rootCat.substring(0, rootCat.indexOf(';'));
			}
		}
		return rootCat;
	}
}
