package com.yihaodian.bi.storm.monitor;

import java.util.Calendar;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yihaodian.bi.common.util.DateUtil;
import com.yihaodian.bi.common.util.NetUtil;
import com.yihaodian.bi.hbase.dao.BaseDao;
import com.yihaodian.bi.storm.common.util.Constant;

public class PVUV extends BaseMonitor{
	//private static final Logger logger = LoggerFactory.getLogger(PVUV.class);

	private static String TOPIC = "[STORM监控][PVUV]";
	
	private Calendar cal ;
	private Date currentDate;
	private Date lastWeekDate;
	private String calls;
	private float threshold;
	private BaseDao dao;
	
	public PVUV() {
		
		//dao = getHBase().getOrderDao();
		cal = Calendar.getInstance();
		cal.add(Calendar.MINUTE, -2);
		currentDate = cal.getTime();
		cal.add(Calendar.DATE, -7);
		lastWeekDate = cal.getTime();
	}

	@Override
	public void monitor(String[] args) throws Exception {
		
		threshold = Float.parseFloat(args[0]);
		
		dao = getHBase().getOrderDao();
		check("uv");
		check("pv");

	}

	@Override
	public String getMonitorName() {
		// TODO Auto-generated method stub
		return "PVUV";
	}
	
	public void check(String type) throws Exception {
		
		type = type.toLowerCase();
		
		if (!type.equals("pv") && !type.equals("uv")) {
			throw new IllegalArgumentException("The argument is either 'pv' or 'uv'");
		}
		
		//  监控上周同期
		String rkLastWeek = DateUtil.getFmtDate(lastWeekDate, DateUtil.YYYYMMDD_STR) 
				+ "_" + type + "_" + DateUtil.getFmtDate(lastWeekDate, DateUtil.HHMM);
		//  监控当天数据变化趋势
		String rkLastMin = DateUtil.getFmtDate(currentDate, DateUtil.YYYYMMDD_STR) 
				+ "_" + type + "_" + DateUtil.getFmtDate(currentDate, DateUtil.HHMM);
		//  监控当前值
		String rkNow = "last_tracker_" + type + "_" + DateUtil.getFmtDate(currentDate, DateUtil.YYYYMMDD_STR);
		
		String tbName = type.equals("uv") ? Constant.TABLE_TRACKER_UV_RESULT : Constant.TABLE_TRACKER_PV_RESULT;
		//long rsLastWeek = Long.parseLong(dao.getColumnValue(Constant.TABLE_TRACKER_UV_RESULT, rkLastWeek, Constant.COMMON_FAMILY, "tracker_uv"));
		// 由于缺少数据， 暂时用2min前数据代替 上周同期
		long valLastWeek = Long.parseLong(dao.getColumnValue(tbName, rkLastWeek, Constant.COMMON_FAMILY, "tracker_"+ type));
		long valLastMin = Long.parseLong(dao.getColumnValue(tbName, rkLastMin, Constant.COMMON_FAMILY, "tracker_" + type));
		long valNow = Long.parseLong(dao.getColumnValue(tbName, rkNow, Constant.COMMON_FAMILY, "tracker_" + type));
		

		String msg = "";
		if (valNow == 0) {
		  msg = String.format("危险！！目前Storm全站-" + type + "-当天-为0");
		}
		else if (valLastMin == 0) {
			msg = String.format("危险！！目前Storm全站-" + type + "-" + valLastMin  + "-为0");
		}
		else if (valLastWeek == 0) {
	      msg = String.format("危险！！目前Storm全站-" + type + "-上周同期-为0");
		}
		else {
			if (valNow - valLastMin < 0) {
				 msg = String.format("危险！！目前Storm全站" + type + "正在下降");
			}
			
			double persent = (double)(valNow - valLastWeek) / valLastWeek;
			if ( Math.abs(persent) > this.threshold) {
				msg("Storm UV now :" + valNow); 
				msg("Storm UV the same time of last week: " + valLastWeek);
	            msg = String.format("目前Storm全站-" + type + "="+valNow+  ",上周同比="+valLastWeek+   "相差 %.1f%%", persent*100);
	            msg(msg);
			}
		}
		
		if (!msg.isEmpty()) {
			sendMessageByCache(msg,TOPIC+type);
		}
	}
	
	
	private void printUsage() {
		msg(getClass().getName() + " CallNumber Threshold\n\n"
				+ "CallNumber        Mobile number receiving WARNING message, seperated by comma.\n"
				+ "Threshold         Tolerence threshold ");
	}

	@Override
	public void help() {
		msg("全站PV和UV数据监控");
		msg("1. 监控规则：");
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
			try {
				new PVUV().doMonitor(args);
			} catch (Exception e) {
				msg("error. Caused by:"  + e.getMessage());
			}
	}
}
