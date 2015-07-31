package com.yihaodian.bi.storm.common.model;

import java.io.Serializable;
/**
 * 用于存储一个点的属性
 * */
public class CharDataVO  implements Serializable{

	private static final long serialVersionUID = 1L;
	
	private String x_value;//x轴的坐标
	
	private String y_value;//y轴的坐标
	
	private String p_title;//点的标题
	
	private String x_column;//数据库中存储x轴坐标的字段名
	
	private String y_column;//数据库中存储y轴的坐标的字段名
	
	private String pt_column;//数据库中存储标题的字段名
	
	private String title_pre;//标题前缀
	private String title_suf;//标题后缀
	
	public CharDataVO(){
	}
	/**
	 * 获取要查询的字段
	 * */
	public String[] getColumns(){
		String[] columns={this.getX_column(),this.getY_column(),this.getPt_column()};
		return columns;
	}
	public CharDataVO(String x_column,String y_column,String pt_column){
		this.x_column=x_column;
		this.y_column=y_column;
		this.pt_column=pt_column;
	}
	
	public void clearData(){
		this.x_value=null;
		this.y_value=null;
		this.p_title=null;
	}
	
	public String getX_value() {
		return x_value;
	}
	public void setX_value(String x_value) {
		this.x_value = x_value;
	}
	/**
	 * 如果标题为空，则以x轴为标题
	 * */
	public String getP_title() {
		if(this.p_title==null||this.p_title.equals("")){
			this.p_title=this.x_value;
		}
		return this.p_title;
	}
	/**
	 * 如果标题为空，则以x轴为标题，并且自动加上前后缀
	 * */
	public String getP_title_ps() {
		String pt=this.p_title;
		if(this.p_title==null||this.p_title.equals("")){
			pt=this.x_value;
		}
		if(this.title_pre!=null&&!this.title_pre.equals("")&&pt!=null){
			pt=this.title_pre+pt;
		}
		if(this.title_suf!=null&&!this.title_suf.equals("")&&pt!=null){
			pt=pt+this.title_suf;
		}
		return pt;
	}
	public void setP_title(String p_title) {
		this.p_title = p_title;
	}
	public String getY_value() {
		return y_value;
	}
	public void setY_value(String y_value) {
		this.y_value = y_value;
	}
	public String getX_column() {
		return x_column;
	}
	public void setX_column(String x_column) {
		this.x_column = x_column;
	}
	public String getY_column() {
		return y_column;
	}
	public void setY_column(String y_column) {
		this.y_column = y_column;
	}
	/**
	 * 如果标题字段为空，以x轴字段为标题字段
	 * */
	public String getPt_column() {
		if(this.pt_column==null||this.pt_column.equals("")){
			this.pt_column=this.x_column;
		}
		return pt_column;
	}
	public void setPt_column(String pt_column) {
		this.pt_column = pt_column;
	}
	public String getTitle_pre() {
		return title_pre;
	}
	public void setTitle_pre(String title_pre) {
		this.title_pre = title_pre;
	}
	public String getTitle_suf() {
		return title_suf;
	}
	public void setTitle_suf(String title_suf) {
		this.title_suf = title_suf;
	}
	
	

}
