package com.yihaodian.bi.storm.common.util;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class URLUtils {
	
	public static List<String> country=new ArrayList<String>();
	
	public static final String IP_REGEX="([1-9]|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])(\\.(\\d|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])){3}";
	
	static {
		country.add("cn");
		country.add("hk");
		country.add("us");
	}	
	
	public static String getUrlParam(final String url, String param) {
		if (url == null || url.trim().length()==0 
				||param == null ||param.trim().length()==0) { return null; }
		
		String temp=url.trim();
		param=param.trim();
		String[] prefix={"&", "?", "#"};
		boolean include = false;
		for(String p:prefix){
			int index= temp.indexOf(p + param + "=");
			if(index!=-1){
				temp=temp.substring(index+2+param.length());
				include = true;
				break;
			}
		}
		if(!include){
			return null;
		}
		
		if(temp.startsWith("&")|| temp.startsWith("?")){
			return null;
		}
		
		if(temp.contains("&")){
			temp=temp.split("&")[0];
		}
		if(temp.contains("?")){
			temp=temp.split("[?]")[0];
		}
		return temp;
	}	
	
	

	public static String getHostFromURL(String s) {
		if (s == null||s.trim().length()==0) { return null; }

		String temp=s.trim();
		String[] prefix={"http://", "https://","ftp://"};
		for(String p:prefix){
			if(temp.startsWith(p)){
				temp=temp.substring(p.length()).trim();
			}
		}
		
		String host = StringUtils.multipleSplit(temp, "/", "[?]", ":", "%");
		if(host==null){
			return "Invalid URL";
		}

		if(host.matches(IP_REGEX)){
			return host;
		}		
		String[] field = host.split("[.]");
		int len=field.length;
		if(field.length<2)
			return host;
		
		if((country.contains(field[len-1].toLowerCase())) && len>2 && (!field[len-3].equals("www"))){
			return field[len-3]+"."+field[len-2] +"."+ field[len-1];
		}else{
			return field[len-2]+"."+field[len-1];
		}
	}
	
	public static String getPmID(String url) 
	{

		if (url==null || url.length()<10) {
			return null;
		}
		String result = null;
		try{
			url = url.replace("\\", "").toLowerCase().trim();
			Pattern p1 = null;
			Matcher m1 = null;
			if(url.matches("http://www.yihaodian.com/product/[0-9]+_[0-9]+.*") 
					|| url.matches("http://www.1mall.com/product/[0-9]+_[0-9]+.*")
					|| url.matches("http://www.111.com.cn/product/[0-9]+_[0-9]+.*")
					|| url.matches("http://www.yihaodian.com/item/[0-9]+_[0-9]+.*") 
					|| url.matches("http://www.yhd.com/item/[0-9]+_[0-9]+.*") 
					|| url.matches("http://www.1mall.com/item/[0-9]+_[0-9]+.*")
					|| url.matches("http://www.111.com.cn/item/[0-9]+_[0-9]+.*")
					|| url.matches("http://item(-home)?.yihaodian.com/item/[0-9]+_[0-9]+.*") 
					|| url.matches("http://item(-home)?.yhd.com/item/[0-9]+_[0-9]+.*")
					|| url.matches("http://item(-home)?.1mall.com/item/[0-9]+_[0-9]+.*")
	//				|| url.matches("http://item(-home)?.111.com.cn/item/[0-9]+_[0-9]+.*")
					){
				p1=Pattern.compile("[0-9]+_[0-9]+");
				m1=p1.matcher(url);
				if (m1.find()) {
					result = m1.group().split("_")[0];
				}
			}else if(url.matches("http://item(-home)?.yhd.com/item/[0-9]+.*") || url.matches("http://m.yhd.com/item/[0-9]+.*")){
				p1=Pattern.compile("yhd.com/item/([0-9]+)");
				m1=p1.matcher(url);
				if (m1.find()) {
					result = m1.group(1);
				}
			}else if(url.matches("http://item(-home)?.yhd.com/[0-9]+.*")){
				p1=Pattern.compile("yhd.com/([0-9]+)");
				m1=p1.matcher(url);
				if (m1.find()) {
					result = m1.group(1);
				}
			}else if(url.matches("http://www.yihaodian.com/item/lp/[0-9]+_[0-9]+.*")
					||url.matches("http://item.yihaodian.com/item/lp/[0-9]+_[0-9]+.*")
					||url.matches("http://item.yhd.com/item/lp/[0-9]+_[0-9]+.*")
					||url.matches("http://www.yhd.com/item/lp/[0-9]+_[0-9]+.*")
				    ||url.matches("http://m.yhd.com/item/lp/[0-9]+_[0-9]+.*")){
				p1=Pattern.compile("[0-9]+_[0-9]+");
				m1=p1.matcher(url);
				if (m1.find()) {
					result = m1.group().split("_")[1];
				}
			}else if(url.matches("http://s.1mall.com/item/[0-9]+_[0-9]+/.*") || url.matches("http://s.yhd.com/item/[0-9]+_[0-9]+/.*")){
				p1=Pattern.compile("item/[0-9]+_[0-9]+");
				m1=p1.matcher(url);
				if (m1.find()) {
					result = m1.group().split("_")[1];
				}
			}else if(url.matches("http://m.yhd.com/flashproductdetail_[0-9]+_[0-9]+.*")){
				p1=Pattern.compile("flashproductdetail_[0-9]+_[0-9]+");
				m1=p1.matcher(url);
				if (m1.find()) {
					result = m1.group().split("_")[2];
				}
			}else if(url.matches("http://m.yhd.com/mingpin/item/[0-9]+_[0-9]+.*")){
				p1=Pattern.compile("yhd.com/mingpin/item/[0-9]+_[0-9]+");
				m1=p1.matcher(url);
				if (m1.find()) {
					result = m1.group().split("_")[1];
				}
			}else if(url.matches("http://m.yhd.com/item/desc-[0-9]+.*")){
				p1=Pattern.compile("item/desc-([0-9]+)");
				m1=p1.matcher(url);
				if (m1.find()) {
					result = m1.group(1);
				}
			}
		} catch (Exception e) {
			return result;
		}
		return result;
	}
	
	public static void main(String[] argc){
		
		System.out.println(URLUtils.getPmID("http://item.yhd.com/item/1557861?tc=3.0.5.1557861.1&tp=52.21313.108.0.9.VgnmDN"));
		
	}

	
}
