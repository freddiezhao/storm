package com.yihaodian.bi.storm.common.model;

public class HotWordsVo {
	private int hour;
	private int min;
	private String page_value;//搜词名
	private long kw_sec_vstrs;//搜词二跳visits
	private int kw_rate;//详情页转化率
	private int order_num;//搜词排序号
	
	public int getOrder_num() {
		return order_num;
	}
	public void setOrder_num(int order_num) {
		this.order_num = order_num;
	}
	public int getHour() {
		return hour;
	}
	public void setHour(int hour) {
		this.hour = hour;
	}
	public int getMin() {
		return min;
	}
	public void setMin(int min) {
		this.min = min;
	}
	public String getPage_value() {
		return page_value;
	}
	public void setPage_value(String page_value) {
		this.page_value = page_value;
	}
	public long getKw_sec_vstrs() {
		return kw_sec_vstrs;
	}
	public void setKw_sec_vstrs(long kw_sec_vstrs) {
		this.kw_sec_vstrs = kw_sec_vstrs;
	}
	public int getKw_rate() {
		return kw_rate;
	}
	public void setKw_rate(int kw_rate) {
		this.kw_rate = kw_rate;
	}
	
}
