package com.yihaodian.bi.storm.monitor;

import java.util.Calendar;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yihaodian.bi.common.util.DateUtil;
import com.yihaodian.bi.hbase.dao.BaseDao;
import com.yihaodian.bi.storm.common.util.Constant;

public class PVUVbyPlatform extends BaseMonitor{
	//private static final Logger logger = LoggerFactory.getLogger(PVUVbyPlatform.class);

	private static String TOPIC = "[STORM监控][平台PVUV]";
	
	private Calendar cal ;
	private Date currentDate;
	private Date lastWeekDate;
	private String calls;
	private float threshold;
	private BaseDao dao;
	
	public PVUVbyPlatform() {
		
		cal = Calendar.getInstance();
		cal.add(Calendar.MINUTE, -2);
		currentDate = cal.getTime();
		cal.add(Calendar.DATE, -7);
		lastWeekDate = cal.getTime();
	}

	@Override
	public void monitor(String[] args) throws Exception {
		
		this.threshold = Float.parseFloat(args[0]);
		
		dao = getHBase().getOrderDao();
		check("app");
		check("h5");

	}

	@Override
	public String getMonitorName() {
		// TODO Auto-generated method stub
		return "PVUVbyPlatform";
	}
	
	public void check(String type) throws Exception {
		
		type = type.toLowerCase();
		
		if (!type.equals("app") && !type.equals("h5")) {
			throw new IllegalArgumentException("The argument is either 'app' or 'h5'");
		}
		
		//  监控上周同期
		String rkLastWeek = DateUtil.getFmtDate(lastWeekDate, DateUtil.YYYYMMDD_STR) 
				+ "_" + type + "_uv_" + DateUtil.getFmtDate(lastWeekDate, DateUtil.HHMM);
		//  监控当天数据变化趋势
		String rkLastMin = DateUtil.getFmtDate(currentDate, DateUtil.YYYYMMDD_STR) 
				+ "_" + type + "_uv_" + DateUtil.getFmtDate(currentDate, DateUtil.HHMM);
		//  监控当前值
		String rkNow = "last_tracker_" + type + "_uv_" + DateUtil.getFmtDate(currentDate, DateUtil.YYYYMMDD_STR);
		
		//long rsLastWeek = Long.parseLong(dao.getColumnValue(Constant.TABLE_TRACKER_UV_RESULT, rkLastWeek, Constant.COMMON_FAMILY, "tracker_uv"));
		// 由于缺少数据， 暂时用2min前数据代替 上周同期
		long valLastWeek = Long.parseLong(dao.getColumnValue(Constant.TABLE_TRACKER_UV_RESULT, rkLastWeek, Constant.COMMON_FAMILY, "tracker_uv"));
		long valLastMin = Long.parseLong(dao.getColumnValue(Constant.TABLE_TRACKER_UV_RESULT, rkLastMin, Constant.COMMON_FAMILY, "tracker_uv"));
		long valNow = Long.parseLong(dao.getColumnValue(Constant.TABLE_TRACKER_UV_RESULT, rkNow, Constant.COMMON_FAMILY, "tracker_uv"));
		

		String msg = "";
		if (valNow == 0) {
		  msg = String.format("危险！！目前Storm全站UV-" + type + "-当天-为0");
		}
		else if (valLastMin == 0) {
			msg = String.format("危险！！目前Storm全站UV-" + type + "-" + valLastMin  + "-为0");
		}
		else if (valLastWeek == 0) {
	      msg = String.format("危险！！目前Storm全站UV-" + type + "-上周同期-为0");
		}
		else {
			if (valNow - valLastMin < 0) {
				 msg = String.format("危险！！目前Storm全站UV-" + type + "-正在下降");
			}
			
			double persent = (double)(valNow - valLastWeek) / valLastWeek;
			if ( Math.abs(persent) > this.threshold) {
				msg("Storm UV now :" + valNow); 
				msg("Storm UV the same time of last week: " + valLastWeek);
	            msg = String.format("目前Storm全站UV-" + type + "="+valNow+  ",上周同比="+valLastWeek+   "相差 %.1f%%", persent*100);
	            msg(msg);
			}
		}
		
		if (!msg.isEmpty()) {
			sendMessageByCache(msg,TOPIC+type);
		}
	}
	
	private void printUsage() {
		System.out.println(getClass().getName() + " CallNumber Threshold\n\n"
				+ "CallNumber        Mobile number receiving WARNING message, seperated by comma.\n"
				+ "Threshold         Tolerence threshold ");
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		try {
			new PVUVbyPlatform().doMonitor(args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			msg("error. Caused by:"  + e.getMessage());
		}

	}

	@Override
	public void help() {
		// TODO Auto-generated method stub
		
	}
}
