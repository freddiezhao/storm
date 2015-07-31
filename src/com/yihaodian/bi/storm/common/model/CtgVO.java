package com.yihaodian.bi.storm.common.model;

public class CtgVO {

	private String ctgId ;
	private String ctgCnName;
	private String deptId ;
	private String customer_num ;
	private String order_amount ;
	private String order_num ;
	private String sku_num ;
	
	public String getCtgCnName() {
		return ctgCnName;
	}
	public void setCtgCnName(String ctgCnName) {
		this.ctgCnName = ctgCnName;
	}
	public String getCtgId() {
		return ctgId;
	}
	public void setCtgId(String ctgId) {
		this.ctgId = ctgId;
	}
	public String getDeptId() {
		return deptId;
	}
	public void setDeptId(String deptId) {
		this.deptId = deptId;
	}
	public String getCustomer_num() {
		return customer_num;
	}
	public void setCustomer_num(String customerNum) {
		customer_num = customerNum;
	}
	public String getOrder_amount() {
		return order_amount;
	}
	public void setOrder_amount(String orderAmount) {
		order_amount = orderAmount;
	}
	public String getOrder_num() {
		return order_num;
	}
	public void setOrder_num(String orderNum) {
		order_num = orderNum;
	}
	public String getSku_num() {
		return sku_num;
	}
	public void setSku_num(String skuNum) {
		sku_num = skuNum;
	}
	
	
	
	
}
