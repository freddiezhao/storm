package com.yihaodian.bi.storm.common.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertiesUtil {

	/**
	 * 获取属性文件值 支持动态更新properties属性值
	 * 
	 * @param fileName 文件名 src目录下
	 * @param pId 键
	 * @return
	 */
	public static String getPropertiesValue(String fileName, String pId) {
		Properties prop = new Properties();
		String path = PropertiesUtil.class.getClassLoader().getResource(
				fileName).getPath();
		InputStream is = null;
		try {
			is = new FileInputStream(path);
			prop.load(is);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (is != null) {
				try {
					is.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return prop.getProperty(pId);
	}

	/**
	 * 修改Properties的值
	 * @param fileName 文件名
	 * @param pid 键
	 * @param newValue 新值
	 */
	public static void updatePropertiesValue(String fileName, String pid,
			String newValue) {
		String path = PropertiesUtil.class.getClassLoader().getResource(
				fileName).getPath();
		File file = new File(path);
		FileInputStream fis = null;
		FileOutputStream fos = null;
		try {
			fis = new FileInputStream(file);
			Properties prop = new OrderedProperties();
			prop.load(fis);
			prop.setProperty(pid, newValue);
			fos = new FileOutputStream(file);
			prop.store(fos, null);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				fis.close();
				fos.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) {
		
	}
}
