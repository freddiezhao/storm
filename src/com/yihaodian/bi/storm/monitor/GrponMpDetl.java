package com.yihaodian.bi.storm.monitor;

import java.sql.ResultSet;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Date;

import com.yihaodian.bi.common.util.DateUtil;

/**
 * 团闪小口径流量明细监控
 * 
 * @author lining2
 */
public class GrponMpDetl extends BaseMonitor {
	public static final String TOPIC = "[STORM监控][团闪明细数据]";
	public static final String STORM_TRACK_TABLE = "rpt.rpt_realtime_gm_track_detl";
	public static final String STORM_ORDER_TABLE ="rpt.rpt_realtime_gm_ordr_detl";
	private static NumberFormat NF = NumberFormat.getInstance();
	private static double PERCENT = 0.09D;

	static {
		NF.setGroupingUsed(false);
	}

	@Override
	public void monitor(String[] args) throws Exception {
		String curDate = DateUtil.transferDateToString(DateUtil.getDate(),
				DateUtil.YYYY_MM_DD_STR);
		String calls = "";
		if (args.length >= 1) {
			PERCENT = Double.parseDouble(args[0]);
		}
		if (args.length >= 2) {
			curDate = args[1];
		}
		monitorTable(STORM_TRACK_TABLE, curDate, calls, "grpon");// 团购流量
		monitorTable(STORM_ORDER_TABLE, curDate, calls, "grpon");// 团购订单
		monitorTable(STORM_ORDER_TABLE, curDate, calls, "mp");// 闪购订单
		monitorTable(STORM_TRACK_TABLE, curDate, calls, "mp");// 闪购流量
	}

	protected void monitorTable(String table, String curDate, String calls,
			String type) throws Exception {
		long time = DateUtil.getDate().getTime() - 2 * 60 * 1000;
		Date date1 = new Date(time);
		String curMin = DateUtil.transferDateToString(date1,DateUtil.YYYY_MM_DD_HH_MM_STR);
		Statement st = oracleCon.createStatement();
		String sql = "";
		String str="";
		if(type.equals("grpon")&& table.equals(STORM_TRACK_TABLE) ) {
			str=" AND x.track_source IN (101, 102, 103)";
		}else if(type.equals("mp")&& table.equals(STORM_TRACK_TABLE)) {
			str=" AND x.track_source IN (201, 202, 203)";
		}else if(type.equals("mp")&& table.equals(STORM_ORDER_TABLE)) {
			str=" AND prmtn_rule_type=7";
		}else if(type.equals("grpon")&& table.equals(STORM_ORDER_TABLE)) {
			str=" AND prmtn_rule_type=2";
		}
		if (type.equals("grpon")) {
			sql = "SELECT to_char(date_time_id, 'yyyy-mm-dd hh24:mi') as curMin ,COUNT(*) AS num\n"
					+ "FROM   "+table+" x\n"
					+ "WHERE  to_char(date_time_id, 'yyyy-mm-dd hh24:mi')= '"
					+ curMin
					+ "'\n"
					+ ""+str+"\n"
					+ "group by to_char(date_time_id, 'yyyy-mm-dd hh24:mi')";
		} else if (type.equals("mp")) {
			sql = "SELECT    to_char(date_time_id, 'yyyy-mm-dd hh24:mi') as curMin, COUNT(*) AS num\n"
					+ "FROM   "+table+" x\n"
					+ "WHERE  to_char(date_time_id, 'yyyy-mm-dd hh24:mi')= '"
					+ curMin
					+ "'\n"
					+ ""+str+"\n"
					+ "group by to_char(date_time_id, 'yyyy-mm-dd hh24:mi')";
		}

		msg(sql);
		ResultSet rs = st.executeQuery(sql);
		int count = 0;
		while (rs.next()) {
			count++;
		}
		if (count == 0 && type.equals("grpon") && table.equals(STORM_TRACK_TABLE) ) {
			String msg = "在 " + curMin + ", 没有团购流量明细数据，查看生成"+table+"表是否正常!";
			sendMessageByCache(msg, TOPIC);
		}else if (count==0 && type.equals("mp") && table.equals(STORM_ORDER_TABLE)) {
			String msg = "在 " + curMin + ", 没有闪购订单明细数据，查看生成"+table+"表是否正常!";
			sendMessageByCache(msg, TOPIC);
		}else if (count==0 && type.equals("mp") && table.equals(STORM_TRACK_TABLE)) {
			String msg = "在 " + curMin + ", 没有闪购流量明细数据，查看生成"+table+"表是否正常!";
			sendMessageByCache(msg, TOPIC);
		}
		else if (count==0 && type.equals("grpon") && table.equals(STORM_ORDER_TABLE)) {
			String msg = "在 " + curMin + ", 没有团购订单明细数据，查看生成"+table+"表是否正常!";
			sendMessageByCache(msg, TOPIC);
		}
	}

	@Override
	public String getMonitorName() {
		return "RptRealtimeGrponMp-monitor";
	}

	public static void main(String[] args) throws Exception {
		GrponMpDetl m = new GrponMpDetl();
		m.doMonitor(args);
	}

	@Override
	public void help() {
		msg("标准成交口径 销售净额监控");
		msg("\t1.当前分钟没有团闪数据，则报警");
	}
}
