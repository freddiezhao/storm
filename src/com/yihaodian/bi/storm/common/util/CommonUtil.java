package com.yihaodian.bi.storm.common.util;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;

import com.yihaodian.bi.common.util.DateUtil;
import com.yihaodian.bi.hbase.dao.BaseDao;
import com.yihaodian.bi.storm.common.model.CharDataVO;

public class CommonUtil {

	/**
	 * @param args
	 */
	public static DecimalFormat df = new DecimalFormat("###0.00");

	public static DecimalFormat format_noPoint = new DecimalFormat("#");

	public static Double getFmatDouble(Double value) {
		return Double.parseDouble(df.format(value));
	}

	public static String getFmatString(Object value) {
		return df.format(value);
	}

	/**
	 * 计算x轴的初始化坐标
	 * 
	 * @return
	 */
	public static Double getXValue() {
		Calendar c = Calendar.getInstance();
		c.setTime(new java.util.Date());
		int hour = c.get(Calendar.HOUR_OF_DAY);
		int minute = c.get(Calendar.MINUTE);
		int curSecNum = hour * 60 + minute;
		Double xValue = getFmatDouble((double) curSecNum / 60);
		return xValue;
	}

	/**
	 * 计算两分钟前x轴的初始化坐标
	 * 
	 * @return
	 */
	public static Double getLastXValue() {
		Calendar c = Calendar.getInstance();
		c.setTime(new java.util.Date());
		int hour = c.get(Calendar.HOUR_OF_DAY);
		int minute = c.get(Calendar.MINUTE) - 2;
		int curSecNum = hour * 60 + minute;
		Double xValue = getFmatDouble((double) curSecNum / 60);
		return xValue;
	}

	public static Double getStepXValue(String yyyyMMddHHmm, int step) {
		Calendar c = Calendar.getInstance();
		SimpleDateFormat sdf = new SimpleDateFormat(DateUtil.YYYYMMDDHHMM_STR);
		try {
			if (yyyyMMddHHmm != null) {
				c.setTime(sdf.parse(yyyyMMddHHmm));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		int hour = c.get(Calendar.HOUR_OF_DAY);
		int minute = c.get(Calendar.MINUTE) - 2;
		int curSecNum = hour * 60 + minute;
		Double xValue = getFmatDouble((double) curSecNum / 60);
		return xValue;
	}

	/**
	 * 计算两分钟前x轴的初始化坐标 补充上当前日期时间分钟
	 * 
	 * @return
	 */
	public static String[] getLastXValueStr() {
		String lm = DateUtil.getLastMinute();// 获得前2分钟，如201405051651

		int hour = Integer.parseInt(lm.substring(8, 10));
		int minute = Integer.parseInt(lm.substring(10, 12));
		int curSecNum = hour * 60 + minute;
		Double xValue = getFmatDouble((double) curSecNum / 60);
		String str_xValue = xValue.toString();
		String[] end = { lm.substring(8, 10) + "." + lm.substring(10, 12),
				str_xValue };
		return end;
	}

	/**
	 * 计算当前x轴的初始化坐标 补充上当前日期时间分钟
	 * 
	 * @return
	 */
	public static String[] getCurXValueStr() {
		String lm = DateUtil.getCurMinute();

		int hour = Integer.parseInt(lm.substring(8, 10));
		int minute = Integer.parseInt(lm.substring(10, 12));
		int curSecNum = hour * 60 + minute;
		Double xValue = getFmatDouble((double) curSecNum / 60);
		String str_xValue = xValue.toString();
		String[] end = { lm.substring(8, 10) + "." + lm.substring(10, 12),
				str_xValue };
		return end;
	}

	/**
	 * 计算当前x轴的初始化坐标 秒级更新
	 * 
	 * @return
	 */
	public static String[] getCurFullXValueStr() {
		Calendar c = Calendar.getInstance();
		int hour = c.get(Calendar.HOUR_OF_DAY);
		int minute = c.get(Calendar.MINUTE);
		int sec = c.get(Calendar.SECOND);
		int curSecNum = hour * 3600 + minute * 60 + sec;

		String lm = DateUtil.getCurMinute();

		Double xValue = (double) curSecNum / 3600;
		String str_xValue = xValue.toString();
		String[] end = { lm.substring(8, 10) + "." + lm.substring(10, 12),
				str_xValue };
		return end;
	}

	/**
	 * 通过订单创建时间或者付款确认时间来计算当前x轴的初始化坐标 秒级更新
	 * 
	 * @return
	 */
	public static String[] getCurFullXValueByOrdrTime(Date ordrTime) {
		Calendar c = Calendar.getInstance();
		c.setTime(ordrTime);
		int hour = c.get(Calendar.HOUR_OF_DAY);
		int minute = c.get(Calendar.MINUTE);
		int sec = c.get(Calendar.SECOND);
		int curSecNum = hour * 3600 + minute * 60 + sec;

		String lm = DateUtil.getMinute(ordrTime);

		Double xValue = (double) curSecNum / 3600;
		String str_xValue = xValue.toString();
		String[] end = { lm.substring(8, 10) + "." + lm.substring(10, 12),
				str_xValue };
		return end;
	}

	/**
	 * 临时方法 处理开始时间从10点开始的情况
	 * 
	 * @return
	 */
	public static String[] getCurFullXValueByOrdrTime1(Date ordrTime) {
		Calendar c = Calendar.getInstance();
		c.setTime(ordrTime);
		int hour = c.get(Calendar.HOUR_OF_DAY);
		int minute = c.get(Calendar.MINUTE);
		int sec = c.get(Calendar.SECOND);
		int curSecNum = hour * 3600 + minute * 60 + sec;

		String lm = DateUtil.getMinute(ordrTime);
		int i = Integer.parseInt(lm.substring(8, 10));
		String showTitle = "";
		if (i < 14) {
			showTitle = i + 10 + "";
		} else {
			showTitle = i - 14 + "";
		}
		Double xValue = (double) curSecNum / 3600;
		String str_xValue = xValue.toString();
		String[] end = { showTitle + "." + lm.substring(10, 12), str_xValue };
		return end;
	}

	public static String[] getCurFullXValueByOrdrTime2(Date ordrTime) {
		Calendar c = Calendar.getInstance();
		c.setTime(ordrTime);
		int hour = c.get(Calendar.HOUR_OF_DAY);
		int minute = c.get(Calendar.MINUTE);
		int sec = c.get(Calendar.SECOND);
		if (hour == 0) {
			hour = 23;
			minute = 59;
		}
		int curSecNum = hour * 3600 + minute * 60 + sec;

		String lm = DateUtil.getMinute(ordrTime);

		Double xValue = (double) curSecNum / 3600;
		String str_xValue = xValue.toString();
		String[] end = { lm.substring(8, 10) + "." + lm.substring(10, 12),
				str_xValue };
		return end;
	}

	public static String[] getCurFullXValueByOrdrTime(String yyyyMMddHHmm,
			int step) {
		Calendar c = Calendar.getInstance();

		SimpleDateFormat sdf = new SimpleDateFormat(DateUtil.YYYYMMDDHHMM_STR);
		try {
			if (yyyyMMddHHmm != null) {
				c.setTime(sdf.parse(yyyyMMddHHmm));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		c.add(Calendar.MINUTE, step);

		int hour = c.get(Calendar.HOUR_OF_DAY);
		int minute = c.get(Calendar.MINUTE);
		int sec = c.get(Calendar.SECOND);
		int curSecNum = hour * 3600 + minute * 60 + sec;

		String lm = sdf.format(c.getTime());

		Double xValue = (double) curSecNum / 3600;
		String str_xValue = xValue.toString();
		String[] end = { lm.substring(8, 10) + "." + lm.substring(10, 12),
				str_xValue };
		return end;
	}

	/**
	 * 转换历史数据 拼凑highchart的初始化数据格式
	 * 
	 * @return
	 */
	public static String transformHistoryData(List<Result> listToday) {
		int listTodaySize = listToday.size();
		StringBuffer data = new StringBuffer();
		String[] strArr = null;
		data.append("\'[");
		int oNum = 0;
		for (Result result : listToday) {
			oNum++;
			data.append("[");
			int num = 0;
			strArr = new String[2];
			for (KeyValue keyValue : result.raw()) {
				num++;
				if (num == 1) {
					strArr[0] = new String(keyValue.getValue());
				}
				if (num == 2) {
					strArr[1] = new String(keyValue.getValue());
				}
			}

			data.append(strArr[1]);
			data.append(",");
			data.append(strArr[0]);

			if (oNum == listTodaySize) {
				data.append("]");
			} else {
				data.append("],");
			}

		}
		data.append("]\'");
		return data.toString();
	}

	/**
	 * 转换历史数据 拼凑highchart的初始化数据格式
	 * 
	 * @return
	 */
	public static String transformHistoryDataNew(List<Result> listToday,
			CharDataVO columnname) {
		int listTodaySize = listToday.size();
		StringBuffer data = new StringBuffer();
		String[] strArr = null;
		data.append("\'[");
		int oNum = 0;
		for (Result result : listToday) {
			oNum++;
			data.append("[");
			strArr = new String[2];
			for (KeyValue keyValue : result.raw()) {
				if (new String(keyValue.getQualifier()).equals(columnname
						.getX_column())) {
					columnname.setX_value(new String(keyValue.getValue()));
				}
				if (new String(keyValue.getQualifier()).equals(columnname
						.getY_column())) {
					columnname.setY_value(new String(keyValue.getValue()));
				}
			}

			data.append(strArr[1]);
			data.append(",");
			data.append(strArr[0]);

			if (oNum == listTodaySize) {
				data.append("]");
			} else {
				data.append("],");
			}

		}
		data.append("]\'");
		return data.toString();
	}

	/**
	 * 得到一个点的json串，如：{name:'name',x:a,y:b}
	 */
	public static String getOnePointJson(CharDataVO columnname) {
		String pt = columnname.getP_title_ps();
		// if(pt==null||pt.equals("")){
		// pt=columnname.getX_value();
		// }
		StringBuffer data = new StringBuffer();
		data.append("\\{");
		data.append("name:\\\\'" + pt + "\\\\'");
		data.append(",x:" + columnname.getX_value());
		data.append(",y:" + columnname.getY_value());
		data.append("\\}");
		return data.toString();
	}

	/**
	 * 从Result中匹配出数据设进vo里
	 */
	public static void setCharData(Result r, CharDataVO columnname) {
		for (KeyValue keyValue : r.raw()) {
			if (new String(keyValue.getQualifier()).equals(columnname
					.getPt_column())) {
				columnname.setP_title(new String(keyValue.getValue()));
			}
			if (new String(keyValue.getQualifier()).equals(columnname
					.getX_column())) {
				columnname.setX_value(new String(keyValue.getValue()));
			}
			if (new String(keyValue.getQualifier()).equals(columnname
					.getY_column())) {
				columnname.setY_value(new String(keyValue.getValue()));
			}
		}
	}

	/**
	 * 转换历史数据 拼凑highchart的初始化数据格式
	 * 
	 * @param columnname
	 *            用来存储要从result中取出的字段，按照
	 * @return
	 */
	public static String transformHistoryData(List<Result> listToday,
			CharDataVO columnname) {
		// var
		// json_str_="'[\{name:\\'name1\\',x:13.28,y:0\},\{name:\\'name2\\',x:13.68,y:5\}]'";
		// var option_={name:'name',x:a,y:b};
		int listTodaySize = listToday.size();
		StringBuffer data = new StringBuffer();
		String[] strArr = null;
		data.append("\'[");
		int oNum = 0;
		for (Result result : listToday) {
			oNum++;
			setCharData(result, columnname);
			if (oNum != 1) {
				data.append(",");
			}
			data.append(getOnePointJson(columnname));
		}
		data.append("]\'");
		return data.toString();
	}

	/**
	 * 补充初始化数据的点 因为初始化数据只加载一次 刷新页面的时候不会重新查询初始数据故需要不断累加
	 * 
	 * @param sBuffer
	 * @param xvalue
	 * @param amtString
	 * @return
	 */
	public static String appendStr(String sBuffer, String xvalue,
			String amtString) {
		if (sBuffer.toString().equals("'[]'")) {
			sBuffer = sBuffer.substring(0, sBuffer.length() - 2);
			return sBuffer + "[" + xvalue + "," + amtString + "]]\'";
		} else {
			sBuffer = sBuffer.substring(0, sBuffer.length() - 2);
			return sBuffer + ",[" + xvalue + "," + amtString + "]]\'";
		}
	}

	/**
	 * 补充初始化数据的点 因为初始化数据只加载一次 刷新页面的时候不会重新查询初始数据故需要不断累加
	 * 
	 * @param sBuffer
	 * @param xvalue
	 * @param amtString
	 * @return
	 */
	public static String appendStr(String sBuffer, CharDataVO columnname) {
		if (sBuffer.toString().equals("'[]'")) {
			sBuffer = sBuffer.substring(0, sBuffer.length() - 2);
			return sBuffer + getOnePointJson(columnname) + "]\'";
		} else {
			sBuffer = sBuffer.substring(0, sBuffer.length() - 2);
			return sBuffer + "," + getOnePointJson(columnname) + "]\'";
		}
	}

	/**
	 * 获取结果表的最后操作时间
	 * 
	 * @param 业务rowkey
	 */
	public static String getLastOptTime(BaseDao dao, String rowkey) {
		Result result = null;
		try {
			result = dao.getOneRecord(Constant.TABLE_DICT, rowkey);
		} catch (IOException e) {
			e.printStackTrace();
		}
		String lastOptTime = null;
		for (KeyValue keyValue : result.raw()) {
			if (new String(keyValue.getQualifier())
					.equals(Constant.COLUMN_DICT_LASTTIME)) {
				lastOptTime = new String(keyValue.getValue());
			}
		}
		return lastOptTime;
	}

	/**
	 * 分小时显示柱状图 拼接下一个小时数据字符串
	 * 
	 * @param hour
	 * @param sFinal
	 * @param sAppend
	 * @return
	 */
	public static String appendNextHourStr(int hour, String sFinal,
			String sAppend) {
		// 打开末尾闭口
		String temp = sFinal.substring(0, sFinal.length() - 1);
		// 累加字符串
		if (!"[]".equals(sFinal)) {
			sFinal = temp + ",[" + hour + "," + sAppend + "]]";
		} else {
			sFinal = "[" + temp + hour + "," + sAppend + "]]";
		}
		return sFinal;
	}

	/**
	 * 分小时显示柱状图 替换当前最后一个小时的值
	 * 
	 * @param sFinal
	 * @param sumValue
	 * @return
	 */
	public static String replaceNowHourStr(String sFinal, String sumValue) {
		if (!"[]".equals(sFinal) && !"".equals(sFinal)
				&& sFinal.indexOf(",") != -1) {
			sFinal = sFinal.substring(0, sFinal.lastIndexOf(",")) + ","
					+ sumValue + "]]";
		}
		return sFinal;
	}

	/**
	 * 分小时显示柱状图 替换之前小时的累加值
	 * 
	 * @param sFinal
	 * @param sumValue
	 * @param orderCreateHour
	 * @return
	 */
	public static String replaceBeforeHourStr(String sFinal, Double sumValue,
			int orderCreateHour) {
		Double rsltAmount = 0.0;
		String tmp = "[" + orderCreateHour + ",";
		String sHead = "";
		String sMid = "";
		String sTail = "";
		String sValue = "0.0";
		if (sFinal.indexOf(tmp) != -1) {
			sHead = sFinal.substring(0, sFinal.indexOf(tmp) + tmp.length());
			sMid = sFinal.substring(sFinal.indexOf(tmp) + tmp.length());
			sValue = sMid.substring(0, sMid.indexOf("]"));
			rsltAmount = Double.valueOf(sValue) + sumValue;
			sTail = sMid.substring(sMid.indexOf("]"));
			sFinal = sHead + rsltAmount + sTail;
		}
		return sFinal;
	}

	/**
	 * 分小时显示柱状图 替换之前小时的累加值 订单数
	 * 
	 * @param sFinal
	 * @param sumValue
	 * @param orderCreateHour
	 * @return
	 */
	public static String replaceOrdrNumBeforeHourStr(String sFinal,
			int orderCreateHour) {
		long rsltOrdrNum = 0;
		int num = 1;
		String tmp = "[" + orderCreateHour + ",";
		String sHead = "";
		String sMid = "";
		String sTail = "";
		String sValue = "0";
		if (sFinal.indexOf(tmp) != -1) {
			sHead = sFinal.substring(0, sFinal.indexOf(tmp) + tmp.length());
			sMid = sFinal.substring(sFinal.indexOf(tmp) + tmp.length());
			sValue = sMid.substring(0, sMid.indexOf("]"));
			rsltOrdrNum = Long.parseLong(sValue) + num;
			sTail = sMid.substring(sMid.indexOf("]"));
			sFinal = sHead + rsltOrdrNum + sTail;
		}
		return sFinal;
	}

	/**
	 * 查询点存入VO
	 * 
	 * @param rowkey
	 * @return
	 */
	public static void getPoint(BaseDao dao, String tableName, String rowkey,
			CharDataVO columnname) {
		System.out.println("query so:" + rowkey);
		Result result = null;
		try {
			result = dao.getOneRecord(tableName, rowkey);
		} catch (IOException e) {
			e.printStackTrace();
		}

		for (KeyValue keyValue : result.raw()) {
			if (new String(keyValue.getQualifier()).equals(columnname
					.getPt_column())) {
				columnname.setP_title(new String(keyValue.getValue()));
			}
			if (new String(keyValue.getQualifier()).equals(columnname
					.getX_column())) {
				columnname.setX_value(new String(keyValue.getValue()));
			}
			if (new String(keyValue.getQualifier()).equals(columnname
					.getY_column())) {
				columnname.setY_value(new String(keyValue.getValue()));
			}
		}
		if (result.size() == 0) {
			System.out.println(rowkey + ": point is null...");
			columnname.setP_title(null);
			columnname.setX_value(null);
			columnname.setY_value(null);
		}
	}

	/**
	 * 拼接json
	 * 
	 * @param list
	 */
	public static String createJsonStr(Map<String, String> map) {
		String jsonStr = "{";
		int n = 0;
		for (String key : map.keySet()) {
			n++;
			if (n == 1) {
				jsonStr += "\\'" + key + "\\':" + map.get(key);
			} else {
				jsonStr += ",\\'" + key + "\\':" + map.get(key);
			}
		}
		jsonStr += "}";
		return jsonStr;
	}

	/**
	 * 针对线图和柱状图混搭的前端5秒读取一次数据使用 add by zhangsheng 20140618
	 * 
	 * @param rowKeyRegexPoint
	 *            最后点的rowkey
	 * @param rowKeyRegexCol
	 *            柱状图rowkey
	 * @param colNameArr
	 *            hbase列名,通过列名匹配查询内容,这里需要写死顺序
	 * @return
	 */
	public static String[] findData(BaseDao dao, String tableName,
			String rowKeyRegexPoint, String rowKeyRegexCol, String[] colNameArr) {
		String xTitle = "";
		String xValue = "";
		String pointData = "";
		String colData = "";
		String[] valueArr = new String[4];
		try {
			// 点
			Result resultPoint = dao.getOneRecord(tableName, rowKeyRegexPoint);
			// 一个result里有多列，取需要的列
			for (KeyValue keyValue : resultPoint.raw()) {
				if (colNameArr[0].equals(new String(keyValue.getQualifier()))) {
					xTitle = new String(keyValue.getValue());
				}
				if (colNameArr[1].equals(new String(keyValue.getQualifier()))) {
					xValue = new String(keyValue.getValue());
				}
				if (colNameArr[2].equals(new String(keyValue.getQualifier()))) {
					pointData = new String(keyValue.getValue());
				}
			}
			// 柱状图
			Result resultCol = dao.getOneRecord(tableName, rowKeyRegexCol);
			for (KeyValue keyValue : resultCol.raw()) {
				if (colNameArr[3].equals(new String(keyValue.getQualifier()))) {
					colData = new String(keyValue.getValue());
					break;
				}
			}
			valueArr[0] = xTitle;
			valueArr[1] = xValue;
			valueArr[2] = pointData;
			valueArr[3] = colData;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return valueArr;
	}

	/**
	 * TODO 判断是否在线支付
	 * 
	 * @param payServiceType
	 * @return
	 */
	public static boolean isPayOnline(Integer payServiceType) {
		boolean b = false;
		if (payServiceType != 2 && payServiceType != 5 && payServiceType != 9
				&& payServiceType != 8 && payServiceType != 10) {
			b = true;
		}
		return b;
	}

	public static void main(String[] args) {
		System.out.println(getCurFullXValueStr()[0] + "==="
				+ getCurFullXValueStr()[1]);
		// System.out.println(CommonUtil.format_noPoint.format("123.5567"));
	}

}
