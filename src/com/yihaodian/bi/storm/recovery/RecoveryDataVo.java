package com.yihaodian.bi.storm.recovery;

public class RecoveryDataVo {
	//20140620_amount_1403193603361,order_amount:400.6,xTitle:00.00,xValue:8.333333333333334E-4
	private String rowkey;
	private String column1;
	private String column2;
	private String xTitle;
	private String xValue;
	public String getRowkey() {
		return rowkey;
	}
	public void setRowkey(String rowkey) {
		this.rowkey = rowkey;
	}
	public String getColumn1() {
		return column1;
	}
	public void setColumn1(String column1) {
		this.column1 = column1;
	}
	public String getColumn2() {
		return column2;
	}
	public void setColumn2(String column2) {
		this.column2 = column2;
	}
	public String getXTitle() {
		return xTitle;
	}
	public void setXTitle(String title) {
		xTitle = title;
	}
	public String getXValue() {
		return xValue;
	}
	public void setXValue(String value) {
		xValue = value;
	}
	
}
