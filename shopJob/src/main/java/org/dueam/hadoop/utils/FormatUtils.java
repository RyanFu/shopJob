package org.dueam.hadoop.utils;

public class FormatUtils {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		System.out.println(number("1234567"));
		System.out.println(number("12345.6790"));

		System.out.println(number("�й�����.�������"));

	}

	/**
	 * ��ʽ�����
	 *
	 * @param cent
	 *            ��λ����
	 */
	public static String money(long cent) {
		return number(String.valueOf((double) cent / 100));
	}

	/**
	 * ��ʽ�����
	 *
	 * @param cent
	 *            ��λ��Ԫ
	 */
	public static String money(String money) {
		return number(String.valueOf(money));
	}

	/**
	 * ��ʽ�����
	 *
	 * @param cent
	 *            ��λ����
	 */
	public static String money(int cent) {
		return number(String.valueOf((double) cent / 100));
	}

	/**
	 * ��ʽ������ 12345.23 => 12,345.23 123456 => 123,456
	 *
	 * @param number
	 * @return
	 */
	public static String number(long num) {
		return number(String.valueOf(num));
	}

	/**
	 * ��ʽ������ 12345.23 => 12,345.23 123456 => 123,456
	 *
	 * @param number
	 * @return
	 */
	public static String number(int num) {
		return number(String.valueOf(num));
	}

	/**
	 * ��ʽ������ 12345.23 => 12,345.23 123456 => 123,456
	 *
	 * @param number
	 * @return
	 */
	public static String number(double num) {
		return number(String.valueOf(num));
	}

	/**
	 * ��ʽ������ 12345.23 => 12,345.23 123456 => 123,456
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
