package com.yihaodian.bi.storm.business.tracker.parse;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TrackerURLParser {
	private static final Logger logger = LoggerFactory.getLogger(TrackerURLParser.class);
	private URL _url;
	private String _host;
	private String _path;
	private static Pattern pattern= Pattern.compile("^\\d+");
	private boolean grponDetl = false;
	private boolean mpDetl = false;
	private boolean grponDetlH5 = false;
	private boolean mpDetlH5 = false;
	
	public TrackerURLParser(String url) throws MalformedURLException{
		    if (url == null) throw new MalformedURLException();
			_url = new URL(url);
			_host = _url.getHost();
			_path = _url.getPath();
			
			if ("t.yhd.com".equals(_host) && _path.startsWith("/detail/"))
				setGrponDetl(true);
			else if ("m.yhd.com".equals(_host) && _path.startsWith("/tuan/detail/"))
				setGrponDetlH5(true);
			else if ("s.yhd.com".equals(_host) && _path.startsWith("/item/")) 
				setMpDetl(true);
			else if ("m.yhd.com".equals(_host) && _path.startsWith("/mingpin/item/"))
				setMpDetlH5(true);
	}
	
	public boolean isGrponDetl() {
		return grponDetl;
	}
	
	public boolean isGrponDetlH5() {
		return grponDetlH5;
	}
	
	public boolean isMpDetl() {
		return mpDetl;
	}
	
	public boolean isMpDetlH5() {
		return mpDetlH5;
	}
	
	public String getGrponID() {
		
		if (isGrponDetl()) {
			String[] tokens = _path.split("/");
			if(tokens.length>=3){
				Matcher matcher = pattern.matcher(tokens[2]);
				if (matcher.find())
				return matcher.group();
			}
		}
		else if (isGrponDetlH5()) {
			String[] tokens = _path.split("/");
			if(tokens.length>=4) {
				Matcher matcher = pattern.matcher(tokens[3]);
				if (matcher.find())
					return matcher.group();
			}
		}
		
		return "";
	}
	
	private void setGrponDetl(boolean bool) {
		grponDetl = bool;
	}
	
	private void setMpDetl(boolean bool) {
		mpDetl = bool;
	}
	
	private void setGrponDetlH5(boolean bool) {
		grponDetlH5 = bool;
	}
	
	private void setMpDetlH5(boolean bool) {
		mpDetlH5 = bool;
	}
}
