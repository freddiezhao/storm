package com.yihaodian.bi.kafka;

import java.io.Serializable;

public class TrackerVO implements Serializable{

	private static final long serialVersionUID = 6152724716114448773L;
	
	String id;
    String url;
    String referer;
    String keyword;
    String type;
    String gu_id;
    String page_id;
    String module_id;
    String link_id;
    String attached_info;
    String session_id;
    String tracker_u;
    String tracker_type;
    String ip;
    String tracker_src;
    String cookie;
    String order_code;
    String track_time;
    String end_user_id;
    String first_link;
    String session_view_no;
    String product_id;
    String merchant_id;
    String province_id;
    String city_id;
    String fee;
    String edm_activity;
    String edm_email;
    String edm_jobid;
    String ie_version;
    String platform;
    String internal_keyword;
    String result_sum;
    String currentpage;
    String link_position;
    String button_position;
    String adgroupkeywordid;
    String ext_field1;
    String ext_field2;
    String ext_field3;
    String ext_field4;
    String ext_field5;
    String ext_field6;
    String ext_field7;
    String ext_field8;
    String ext_field9;
    String ext_field10;
    String pagetypeid;
    String unid;
    String pagevalue;
    String container;

    public TrackerVO(String[] arr){
        this.id = arr[0];
        this.url = arr[1];
        this.referer = arr[2];
        this.keyword = arr[3];
        this.type = arr[4];
        this.gu_id = arr[5];
        this.page_id = arr[6];
        this.module_id = arr[7];
        this.link_id = arr[8];
        this.attached_info = arr[9];
        this.session_id = arr[10];
        this.tracker_u = arr[11];
        this.tracker_type = arr[12];
        this.ip = arr[13];
        this.tracker_src = arr[14];
        this.cookie = arr[15];
        this.order_code = arr[16];
        this.track_time = arr[17];
        this.end_user_id = arr[18];
        this.first_link = arr[19];
        this.session_view_no = arr[20];
        this.product_id = arr[21];
        this.merchant_id = arr[22];
        this.province_id = arr[23];
        this.city_id = arr[24];
        this.fee = arr[25];
        this.edm_activity = arr[26];
        this.edm_email = arr[27];
        this.edm_jobid = arr[28];
        if (arr.length >= 30) this.ie_version = arr[29];
        if (arr.length >= 31) this.platform = arr[30];
        if (arr.length >= 32) this.internal_keyword = arr[31];
        if (arr.length >= 33) this.result_sum = arr[32];
        if (arr.length >= 34) this.currentpage = arr[33];
        if (arr.length >= 35) this.link_position = arr[34];
        if (arr.length >= 36) this.button_position = arr[35];
        if (arr.length >= 37) this.adgroupkeywordid = arr[36];
        if (arr.length >= 38) this.ext_field1 = arr[37];
        if (arr.length >= 39) this.ext_field2 = arr[38];
        if (arr.length >= 40) this.ext_field3 = arr[39];
        if (arr.length >= 41) this.ext_field4 = arr[40];
        if (arr.length >= 42) this.ext_field5 = arr[41];
        if (arr.length >= 43) this.ext_field6 = arr[42];
        if (arr.length >= 44) this.ext_field7 = arr[43];
        if (arr.length >= 45) this.ext_field8 = arr[44];
        if (arr.length >= 46) this.ext_field9 = arr[45];
        if (arr.length >= 47) this.ext_field10 = arr[46];
        if (arr.length >= 48) this.pagetypeid = arr[47];
        if (arr.length >= 49) this.unid = arr[48];
        if (arr.length >= 50) this.pagevalue = arr[49];
        if (arr.length >= 60) this.container = arr[59];
    }

    public String getId() {
        return id;
    }

    public String getUrl() {
        return url;
    }

    public String getReferer() {
        return referer;
    }

    public String getKeyword() {
        return keyword;
    }

    public String getType() {
        return type;
    }

    public String getGu_id() {
        return gu_id;
    }

    public String getPage_id() {
        return page_id;
    }

    public String getModule_id() {
        return module_id;
    }

    public String getLink_id() {
        return link_id;
    }

    public String getAttached_info() {
        return attached_info;
    }

    public String getSession_id() {
        return session_id;
    }

    public String getTracker_u() {
        return tracker_u;
    }

    public String getTracker_type() {
        return tracker_type;
    }

    public String getIp() {
        return ip;
    }

    public String getTracker_src() {
        return tracker_src;
    }

    public String getCookie() {
        return cookie;
    }

    public String getOrder_code() {
        return order_code;
    }

    public String getTrack_time() {
        return track_time;
    }

    public String getEnd_user_id() {
        return end_user_id;
    }

    public String getFirst_link() {
        return first_link;
    }

    public String getSession_view_no() {
        return session_view_no;
    }

    public String getProduct_id() {
        return product_id;
    }

    public String getMerchant_id() {
        return merchant_id;
    }

    public String getProvince_id() {
        return province_id;
    }

    public String getCity_id() {
        return city_id;
    }

    public String getFee() {
        return fee;
    }

    public String getEdm_activity() {
        return edm_activity;
    }

    public String getEdm_email() {
        return edm_email;
    }

    public String getEdm_jobid() {
        return edm_jobid;
    }

    public String getIe_version() {
        return ie_version;
    }

    public String getPlatform() {
        return platform;
    }

    public String getInternal_keyword() {
        return internal_keyword;
    }

    public String getResult_sum() {
        return result_sum;
    }

    public String getCurrentpage() {
        return currentpage;
    }

    public String getLink_position() {
        return link_position;
    }

    public String getButton_position() {
        return button_position;
    }

    public String getAdgroupkeywordid() {
        return adgroupkeywordid;
    }

    public String getExt_field1() {
        return ext_field1;
    }

    public String getExt_field2() {
        return ext_field2;
    }

    public String getExt_field3() {
        return ext_field3;
    }

    public String getExt_field4() {
        return ext_field4;
    }

    public String getExt_field5() {
        return ext_field5;
    }

    public String getExt_field6() {
        return ext_field6;
    }

    public String getExt_field7() {
        return ext_field7;
    }

    public String getExt_field8() {
        return ext_field8;
    }

    public String getExt_field9() {
        return ext_field9;
    }

    public String getExt_field10() {
        return ext_field10;
    }

    public String getPagetypeid() {
        return pagetypeid;
    }

    public String getUnid() {
        return unid;
    }

    public String getPagevalue() {
        return pagevalue;
    }

    public String getContainer() {
        return container;
    }

    @Override
    public String toString() {
        return "Tracker [id=" + id + ", url=" + url + ", referer=" + referer + ", keyword=" + keyword + ", type="
               + type + ", gu_id=" + gu_id + ", page_id=" + page_id + ", module_id=" + module_id + ", link_id="
               + link_id + ", attached_info=" + attached_info + ", session_id=" + session_id + ", tracker_u="
               + tracker_u + ", tracker_type=" + tracker_type + ", ip=" + ip + ", tracker_src=" + tracker_src
               + ", cookie=" + cookie + ", order_code=" + order_code + ", track_time=" + track_time + ", end_user_id="
               + end_user_id + ", first_link=" + first_link + ", session_view_no=" + session_view_no + ", product_id="
               + product_id + ", merchant_id=" + merchant_id + ", province_id=" + province_id + ", city_id=" + city_id
               + ", fee=" + fee + ", edm_activity=" + edm_activity + ", edm_email=" + edm_email + ", edm_jobid="
               + edm_jobid + ", ie_version=" + ie_version + ", platform=" + platform + ", internal_keyword="
               + internal_keyword + ", result_sum=" + result_sum + ", currentpage=" + currentpage + ", link_position="
               + link_position + ", button_position=" + button_position + ", adgroupkeywordid=" + adgroupkeywordid
               + ", ext_field1=" + ext_field1 + ", ext_field2=" + ext_field2 + ", ext_field3=" + ext_field3
               + ", ext_field4=" + ext_field4 + ", ext_field5=" + ext_field5 + ", ext_field6=" + ext_field6
               + ", ext_field7=" + ext_field7 + ", ext_field8=" + ext_field8 + ", ext_field9=" + ext_field9
               + ", ext_field10=" + ext_field10 + ", pagetypeid=" + pagetypeid + ", unid=" + unid + ", pagevalue="
               + pagevalue + ", container=" + container + "]";
    }

}
