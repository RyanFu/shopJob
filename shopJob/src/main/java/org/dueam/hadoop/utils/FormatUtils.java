package org.dueam.hadoop.utils;

public class FormatUtils {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		System.out.println(number("1234567"));
		System.out.println(number("12345.6790"));

		System.out.println(number("中国人骼.外骨骼人"));

	}

	/**
	 * 格式化金额
	 *
	 * @param cent
	 *            单位：分
	 */
	public static String money(long cent) {
		return number(String.valueOf((double) cent / 100));
	}

	/**
	 * 格式化金额
	 *
	 * @param cent
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
	 * @param number
	 * @return
	 */
	public static String number(long num) {
		return number(String.valueOf(num));
	}

	/**
	 * 格式化数字 12345.23 => 12,345.23 123456 => 123,456
	 *
	 * @param number
	 * @return
	 */
	public static String number(int num) {
		return number(String.valueOf(num));
	}

	/**
	 * 格式化数字 12345.23 => 12,345.23 123456 => 123,456
	 *
	 * @param number
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

}
