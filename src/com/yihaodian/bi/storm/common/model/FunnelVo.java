package com.yihaodian.bi.storm.common.model;

public class FunnelVo {
	private int hour;
	private int min;
	private long qz_sec_vstrs;//全站二跳visits
	private long detl_sec_vstrs;//详情页二跳visits
	private long cart_vstrs;//加入购物车visits
	private long ordr_end_vstrs;//订单完成页visits
	private long ordr_num;//所有订单数
	
	private String detl_perct;//详情页占比
	private String cart_perct;//加入购物车占比
	private String ordr_end_perct;//订单完成页占比
	
	private String qz_transf;//全站转化率
	private String detl_transf;//详情页转化率
	private String cart_transf;//加入购物车转化率
	private String ordr_end_transf;//订单完成页转化率
	
	private String lw_qz_sec_perct;//上周同期全站二跳比较增幅
	private String lw_detl_sec_perct;//上周同期详情页二跳比较增幅
	private String lw_cart_perct;//上周同期加入购物车比较增幅
	private String lw_ordr_end_perct;//上周同期订单完成页比较增幅
	
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
	public long getQz_sec_vstrs() {
		return qz_sec_vstrs;
	}
	public void setQz_sec_vstrs(long qz_sec_vstrs) {
		this.qz_sec_vstrs = qz_sec_vstrs;
	}
	public long getDetl_sec_vstrs() {
		return detl_sec_vstrs;
	}
	public void setDetl_sec_vstrs(long detl_sec_vstrs) {
		this.detl_sec_vstrs = detl_sec_vstrs;
	}
	public long getCart_vstrs() {
		return cart_vstrs;
	}
	public void setCart_vstrs(long cart_vstrs) {
		this.cart_vstrs = cart_vstrs;
	}
	public long getOrdr_end_vstrs() {
		return ordr_end_vstrs;
	}
	public void setOrdr_end_vstrs(long ordr_end_vstrs) {
		this.ordr_end_vstrs = ordr_end_vstrs;
	}
	public String getDetl_perct() {
		return detl_perct;
	}
	public void setDetl_perct(String detl_perct) {
		this.detl_perct = detl_perct;
	}
	public String getCart_perct() {
		return cart_perct;
	}
	public void setCart_perct(String cart_perct) {
		this.cart_perct = cart_perct;
	}
	public String getOrdr_end_perct() {
		return ordr_end_perct;
	}
	public void setOrdr_end_perct(String ordr_end_perct) {
		this.ordr_end_perct = ordr_end_perct;
	}
	public String getDetl_transf() {
		return detl_transf;
	}
	public void setDetl_transf(String detl_transf) {
		this.detl_transf = detl_transf;
	}
	public String getCart_transf() {
		return cart_transf;
	}
	public void setCart_transf(String cart_transf) {
		this.cart_transf = cart_transf;
	}
	public String getOrdr_end_transf() {
		return ordr_end_transf;
	}
	public void setOrdr_end_transf(String ordr_end_transf) {
		this.ordr_end_transf = ordr_end_transf;
	}
	public String getLw_qz_sec_perct() {
		return lw_qz_sec_perct;
	}
	public void setLw_qz_sec_perct(String lw_qz_sec_perct) {
		this.lw_qz_sec_perct = lw_qz_sec_perct;
	}
	public String getLw_detl_sec_perct() {
		return lw_detl_sec_perct;
	}
	public void setLw_detl_sec_perct(String lw_detl_sec_perct) {
		this.lw_detl_sec_perct = lw_detl_sec_perct;
	}
	public String getLw_cart_perct() {
		return lw_cart_perct;
	}
	public void setLw_cart_perct(String lw_cart_perct) {
		this.lw_cart_perct = lw_cart_perct;
	}
	public String getLw_ordr_end_perct() {
		return lw_ordr_end_perct;
	}
	public void setLw_ordr_end_perct(String lw_ordr_end_perct) {
		this.lw_ordr_end_perct = lw_ordr_end_perct;
	}
	public long getOrdr_num() {
		return ordr_num;
	}
	public void setOrdr_num(long ordr_num) {
		this.ordr_num = ordr_num;
	}
	public String getQz_transf() {
		return qz_transf;
	}
	public void setQz_transf(String qz_transf) {
		this.qz_transf = qz_transf;
	}
	
}
