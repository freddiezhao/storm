package com.yihaodian.bi.storm.monitor;

import java.sql.ResultSet;
import java.sql.Statement;
import java.text.NumberFormat;
import java.util.Date;

import com.yihaodian.bi.common.util.DateUtil;

/**
 * 标准成交口径 rpt.rpt_realtime_montr_tranx_s监控 （剔除礼品卡，积分换购，减去返利）
 * 
 * @author lining2
 */
public class RptRealtimeMontrCancl extends BaseMonitor {
	public static final String TOPIC = "[STORM监控][标准成交口径取消监控]";
	public static final String STORM_CANCL_TABLE = "rpt.rpt_realtime_montr_cancl_s";
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
		monitorCanclTable(STORM_CANCL_TABLE, curDate, calls);
	}

	protected void monitorCanclTable(String table, String curDate, String calls)
			throws Exception {
		long time = DateUtil.getDate().getTime() - 10 * 60 * 1000;
		long time1 = DateUtil.getDate().getTime();
		Date date = new Date(time);
		Date date1 = new Date(time1);
		String curMin_10 = DateUtil.transferDateToString(date,DateUtil.YYYY_MM_DD_HH_MM_STR)+ ":00";
		String curMin = DateUtil.transferDateToString(date1,DateUtil.YYYY_MM_DD_HH_MM_STR)+ ":00";
		Statement st = oracleCon.createStatement();
		String sql = "SELECT '" + curMin + "' as curMin,\n"
				+ "       COUNT(DISTINCT x.ordr_id) cancl_ordr_num\n"
				+ "FROM   " + STORM_CANCL_TABLE + " x\n"
				+ "WHERE  x.date_time_id >= to_date('" + curMin_10
				+ "', 'yyyy-mm-dd hh24:mi:ss') and x.date_time_id<to_date('"
				+ curMin + "', 'yyyy-mm-dd hh24:mi:ss')";
		msg(sql);
		ResultSet rs = st.executeQuery(sql);
		int count=0;
		while (rs.next()) {
			count++;
		}
		if(count==0) {
			String msg="在"+curMin+",最近10分钟内没有取消成交订单数据,看看生成"+STORM_CANCL_TABLE+"表是否正常!";
			sendMessageByCache(msg, TOPIC);
		}
		
	}

	@Override
	public String getMonitorName() {
		return "RptRealtimeCancl-monitor";
	}

	public static void main(String[] args) throws Exception {
		RptRealtimeMontrCancl m = new RptRealtimeMontrCancl();
		m.doMonitor(args);
	}

	@Override
	public void help() {
		msg("标准成交口径  取消成交订单监控");
		msg("\t1.最近10分钟内没有取消成交订单，则报警");
	}
}
