package com.yihaodian.bi.storm.common.util;

import org.apache.commons.lang.StringUtils;

public class Reverse {

	public static String reverseString(String s) {
		int length = s.length();
		String reverse = ""; // 注意这是空串，不是null
		for (int i = 0; i < length; i++)
			reverse = s.charAt(i) + reverse;// 在字符串前面连接， 而非常见的后面
		return reverse;
	}
	
	public static String reverseString1(String s) {
		char[] array = s.toCharArray();
		String reverse = ""; // 注意这是空串，不是null
		for (int i = array.length - 1; i >= 0; i--)
			reverse += array[i];
		return reverse;
	}
	
	public static String reverseString2(String str) {
		return new StringBuffer(str).reverse().toString();
	}

	/**
	 * test
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		System.out.println(Reverse.reverseString("12333443324"));
		System.out.println(StringUtils.reverse("123456")) ;

	}
}
