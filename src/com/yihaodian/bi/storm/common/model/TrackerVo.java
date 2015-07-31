package com.yihaodian.bi.storm.common.model;

import java.io.Serializable;

/**
 * Tracker 模型类
 * @author lvpeng
 *
 */
public class TrackerVo implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	String id     ;
	String url    ;
	String referer;
	String keyword;
	String type   ;
	String gu_id  ;
	String page_id;
	String module_id      ;
	String link_id;
	String attached_info  ;
	String session_id     ;
	String tracker_u      ;
	String tracker_type   ;
	String ip     ;
	String tracker_src    ;
	String cookie ;
	String order_code     ;
	String track_time     ;
	String end_user_id    ;
	String first_link     ;
	String session_view_no;
	String product_id     ;
	String merchant_id    ;
	String province_id    ;
	String city_id;
	String fee    ;
	String edm_activity   ;
	String edm_email      ;
	String edm_jobid      ;
	String ie_version     ;
	String platform       ;
	String internal_keyword       ;
	String result_sum     ;
	String currentpage    ;
	String link_position  ;
	String button_position;
	String adgroupkeywordid       ;
	String ext_field1     ;
	String ext_field2     ;
	String ext_field3     ;
	String ext_field4     ;
	String ext_field5     ;
	String ext_field6     ;
	String ext_field7     ;
	String ext_field8     ;
	String ext_field9     ;
	String ext_field10    ;
	String pagetypeid     ;
	String unid           ;
	String pagevalue      ;
	String container      ;
	
	public TrackerVo(String content){
		this.setTracker(content) ;
	}
	
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	public String getReferer() {
		return referer;
	}
	public void setReferer(String referer) {
		this.referer = referer;
	}
	public String getKeyword() {
		return keyword;
	}
	public void setKeyword(String keyword) {
		this.keyword = keyword;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public String getGu_id() {
		return gu_id;
	}
	public void setGu_id(String guId) {
		gu_id = guId;
	}
	public String getPage_id() {
		return page_id;
	}
	public void setPage_id(String pageId) {
		page_id = pageId;
	}
	public String getModule_id() {
		return module_id;
	}
	public void setModule_id(String moduleId) {
		module_id = moduleId;
	}
	public String getLink_id() {
		return link_id;
	}
	public void setLink_id(String linkId) {
		link_id = linkId;
	}
	public String getAttached_info() {
		return attached_info;
	}
	public void setAttached_info(String attachedInfo) {
		attached_info = attachedInfo;
	}
	public String getSession_id() {
		return session_id;
	}
	public void setSession_id(String sessionId) {
		session_id = sessionId;
	}
	public String getTracker_u() {
		return tracker_u;
	}
	public void setTracker_u(String trackerU) {
		tracker_u = trackerU;
	}
	public String getTracker_type() {
		return tracker_type;
	}
	public void setTracker_type(String trackerType) {
		tracker_type = trackerType;
	}
	public String getIp() {
		return ip;
	}
	public void setIp(String ip) {
		this.ip = ip;
	}
	public String getTracker_src() {
		return tracker_src;
	}
	public void setTracker_src(String trackerSrc) {
		tracker_src = trackerSrc;
	}
	public String getCookie() {
		return cookie;
	}
	public void setCookie(String cookie) {
		this.cookie = cookie;
	}
	public String getOrder_code() {
		return order_code;
	}
	public void setOrder_code(String orderCode) {
		order_code = orderCode;
	}
	public String getTrack_time() {
		return track_time;
	}
	public void setTrack_time(String trackTime) {
		track_time = trackTime;
	}
	public String getEnd_user_id() {
		if (end_user_id == null)
			end_user_id ="";
		return end_user_id;
	}
	public void setEnd_user_id(String endUserId) {
		end_user_id = endUserId;
	}
	public String getFirst_link() {
		return first_link;
	}
	public void setFirst_link(String firstLink) {
		first_link = firstLink;
	}
	public String getSession_view_no() {
		return session_view_no;
	}
	public void setSession_view_no(String sessionViewNo) {
		session_view_no = sessionViewNo;
	}
	public String getProduct_id() {
		if (product_id == null)
			product_id = "";
		return product_id;
	}
	public void setProduct_id(String productId) {
		product_id = productId;
	}
	public String getMerchant_id() {
		return merchant_id;
	}
	public void setMerchant_id(String merchantId) {
		merchant_id = merchantId;
	}
	public String getProvince_id() {
		return province_id;
	}
	public void setProvince_id(String provinceId) {
		province_id = provinceId;
	}
	public String getCity_id() {
		return city_id;
	}
	public void setCity_id(String cityId) {
		city_id = cityId;
	}
	public String getFee() {
		return fee;
	}
	public void setFee(String fee) {
		this.fee = fee;
	}
	public String getEdm_activity() {
		return edm_activity;
	}
	public void setEdm_activity(String edmActivity) {
		edm_activity = edmActivity;
	}
	public String getEdm_email() {
		return edm_email;
	}
	public void setEdm_email(String edmEmail) {
		edm_email = edmEmail;
	}
	public String getEdm_jobid() {
		return edm_jobid;
	}
	public void setEdm_jobid(String edmJobid) {
		edm_jobid = edmJobid;
	}
	public String getIe_version() {
		return ie_version;
	}
	public void setIe_version(String ieVersion) {
		ie_version = ieVersion;
	}
	public String getPlatform() {
		return platform;
	}
	public void setPlatform(String platform) {
		this.platform = platform;
	}
	public String getInternal_keyword() {
		return internal_keyword;
	}
	public void setInternal_keyword(String internalKeyword) {
		internal_keyword = internalKeyword;
	}
	public String getResult_sum() {
		return result_sum;
	}
	public void setResult_sum(String resultSum) {
		result_sum = resultSum;
	}
	public String getCurrentpage() {
		return currentpage;
	}
	public void setCurrentpage(String currentpage) {
		this.currentpage = currentpage;
	}
	public String getLink_position() {
		return link_position;
	}
	public void setLink_position(String linkPosition) {
		link_position = linkPosition;
	}
	public String getButton_position() {
		return button_position;
	}
	public void setButton_position(String buttonPosition) {
		button_position = buttonPosition;
	}
	public String getAdgroupkeywordid() {
		return adgroupkeywordid;
	}
	public void setAdgroupkeywordid(String adgroupkeywordid) {
		this.adgroupkeywordid = adgroupkeywordid;
	}
	public String getExt_field1() {
		return ext_field1;
	}
	public void setExt_field1(String extField1) {
		ext_field1 = extField1;
	}
	public String getExt_field2() {
		if (ext_field2 == null)
			ext_field2 = "";
		return ext_field2;
	}
	public void setExt_field2(String extField2) {
		ext_field2 = extField2;
	}
	public String getExt_field3() {
		return ext_field3;
	}
	public void setExt_field3(String extField3) {
		ext_field3 = extField3;
	}
	public String getExt_field4() {
		return ext_field4;
	}
	public void setExt_field4(String extField4) {
		ext_field4 = extField4;
	}
	public String getExt_field5() {
		return ext_field5;
	}
	public void setExt_field5(String extField5) {
		ext_field5 = extField5;
	}
	public String getExt_field6() {
		return ext_field6;
	}
	public void setExt_field6(String extField6) {
		ext_field6 = extField6;
	}
	public String getExt_field7() {
		if (ext_field7 == null)
			ext_field7 = "";
		return ext_field7;
	}
	public void setExt_field7(String extField7) {
		ext_field7 = extField7;
	}
	public String getExt_field8() {
		return ext_field8;
	}
	public void setExt_field8(String extField8) {
		ext_field8 = extField8;
	}
	public String getExt_field9() {
		return ext_field9;
	}
	public void setExt_field9(String extField9) {
		ext_field9 = extField9;
	}
	public String getExt_field10() {
		return ext_field10;
	}
	public void setExt_field10(String extField10) {
		ext_field10 = extField10;
	}
	public String getPageTypeId() {
		return pagetypeid;
	}
	public void setPageTypeId(String pagetypeid) {
		this.pagetypeid = pagetypeid;
	}
	public String getUnId() {
		return unid;
	}
	public void setUnId(String unid) {
		this.unid = unid;
	}
	public String getPageValue() {
		if (pagevalue == null)
			pagevalue = "";
		return pagevalue;
	}
	public void setPageValue(String pagevalue) {
		this.pagevalue = pagevalue;
	}
	
	public String getContainer() {
		return container;
	}

	public void setContainer(String container) {
		this.container = container;
	}
	
//	private String line = null;
	private String[] arr;
	public void setTracker(String content)
	{
		if (content != null) {
//			line = content.replaceAll("\"", "");
			arr = content.split("\\t");
			this.setId(arr[0]);
//			System.out.println("id----------------"+this.getId());
			this.setUrl(arr[1]);
			this.setReferer(arr[2]);
			this.setKeyword(arr[3]);
			this.setType(arr[4]);
			this.setGu_id(arr[5]);
			this.setPage_id(arr[6]);
			this.setModule_id(arr[7]);
			this.setLink_id(arr[8]);
			this.setAttached_info(arr[9]);
			this.setSession_id(arr[10]);
			this.setTracker_u(arr[11]);
			this.setTracker_type(arr[12]);
			this.setIp(arr[13]);
			this.setTracker_src(arr[14]);
			this.setCookie(arr[15]);
			this.setOrder_code(arr[16]);
			this.setTrack_time(arr[17]);
			this.setEnd_user_id(arr[18]);
			this.setFirst_link(arr[19]);
			this.setSession_view_no(arr[20]);
			this.setProduct_id(arr[21]);
			this.setMerchant_id(arr[22]);
			this.setProvince_id(arr[23]);
			this.setCity_id(arr[24]);
			this.setFee(arr[25]);
			this.setEdm_activity(arr[26]);
			this.setEdm_email(arr[27]);
			this.setEdm_jobid(arr[28]);
			if(arr.length>=30)this.setIe_version(arr[29]);
			if(arr.length>=31)this.setPlatform(arr[30]);
			if(arr.length>=32)this.setInternal_keyword(arr[31]);
			if(arr.length>=33)this.setResult_sum(arr[32]);
			if(arr.length>=34)this.setCurrentpage(arr[33]);
			if(arr.length>=35)this.setLink_position(arr[34]);
			if(arr.length>=36)this.setButton_position(arr[35]);
			if(arr.length>=37)this.setAdgroupkeywordid(arr[36]);
			if(arr.length>=38)this.setExt_field1(arr[37]);
			if(arr.length>=39)this.setExt_field2(arr[38]);
			if(arr.length>=40)this.setExt_field3(arr[39]);
			if(arr.length>=41)this.setExt_field4(arr[40]);
			if(arr.length>=42)this.setExt_field5(arr[41]);
			if(arr.length>=43)this.setExt_field6(arr[42]);
			if(arr.length>=44)this.setExt_field7(arr[43]);
			if(arr.length>=45)this.setExt_field8(arr[44]);
			if(arr.length>=46)this.setExt_field9(arr[45]);
			if(arr.length>=47)this.setExt_field10(arr[46]);
			if(arr.length>=48)this.setPageTypeId(arr[47]);
			if(arr.length>=49)this.setUnId(arr[48]);
			if(arr.length>=50)this.setPageValue(arr[49]);
			if(arr.length>=60)this.setContainer(arr[59]);
//			System.out.println("id----------------"+this.getSession_id());
		}
	}

}
