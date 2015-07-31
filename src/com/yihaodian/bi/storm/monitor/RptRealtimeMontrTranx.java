package com.yihaodian.bi.storm.monitor;

import java.sql.ResultSet;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Date;

import com.yihaodian.bi.common.util.DateUtil;

/**
 * 标准成交口径 rpt.rpt_realtime_montr_tranx_s监控 （剔除礼品卡，积分换购，减去返利）
 * 
 * @author lining2
 */
public class RptRealtimeMontrTranx extends BaseMonitor {
	public static final String TOPIC = "[STORM监控][标准成交口径]";
	public static final String ORACLE_TABLE = "rpt.rpt_realtime_montr_basics_o";
	public static final String STORM_TABLE = "rpt.rpt_realtime_montr_tranx_s";
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
		monitorTable(STORM_TABLE, curDate, calls);
	}

	protected void monitorTable(String table, String curDate, String calls)
			throws Exception {
		long time = DateUtil.getDate().getTime() - 5 * 60 * 1000;
		Date date1 = new Date(time);
		String curMin = DateUtil.transferDateToString(date1,
				DateUtil.YYYY_MM_DD_HH_MM_STR)
				+ ":00";
		Statement st = oracleCon.createStatement();

		String sql = "SELECT to_date('"
				+ curMin
				+ "','yyyy-mm-dd hh24:mi:ss') as dt,  \n"
				+ "       nvl(s.pm_net_amt,0) AS storm_amt,\n"
				+ "       nvl(c.pm_net_amt,0) AS storm_cancl_amt,\n"
				+ "       nvl(o.pm_net_amt,0) AS oracle_amt,\n"
				+ "       nvl(s.pm_net_amt,0) - nvl(c.pm_net_amt,0) - nvl(o.pm_net_amt,0) AS diff\n"
				+ "\n" + "FROM   (SELECT SUM(s.pm_net_amt) AS pm_net_amt\n"
				+ "             FROM   " + STORM_TABLE + " s\n"
				+ "             WHERE  trunc(date_time_id, 'mi')= to_date('"
				+ curMin + "','yyyy-mm-dd hh24:mi:ss')) s\n"
				+ "LEFT  JOIN (SELECT SUM(o.pm_net_amt) AS pm_net_amt\n"
				+ "        FROM   " + ORACLE_TABLE + " o\n"
				+ "        WHERE  date_time_id = to_date('" + curMin
				+ "', 'yyyy-mm-dd hh24:mi:ss')) o\n" +
				"ON     1 = 1\n"
				+ "LEFT  JOIN (SELECT SUM(c.pm_net_amt) AS pm_net_amt\n"
				+ "             FROM   rpt.rpt_realtime_montr_cancl_s c\n"
				+ "             WHERE  trunc(date_time_id, 'mi')= to_date('"
				+ curMin + "','yyyy-mm-dd hh24:mi:ss')\n"
				+ "             GROUP  BY trunc(c.date_time_id, 'mi')) c\n"
				+ "ON     1 = 1";

		msg(sql);
		ResultSet rs = st.executeQuery(sql);
		int count = 0;
		while (rs.next()) {
			count++;
			String dt = rs.getString(1);
			Double sValue = rs.getDouble(2);
			Double cValue = rs.getDouble(3);
			Double oValue = rs.getDouble(4);
			Double diff = rs.getDouble(5);

			try {
				String key = table + " " + dt;
				msg(key + "\t" + NF.format((sValue - cValue)) + "\t"
						+ NF.format(oValue) + "\t" + diff + "\t"
						+ NF.format(Math.abs(diff) / (sValue - cValue)) + "\n");
				if (table.equals(STORM_TABLE)) {
					stormRule(dt, sValue - cValue, oValue, diff);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		if (count == 0) {
			String msg = "在 " + curMin + ",没有标准成交口径的成交净额 ，查看是否正常!";
			sendMessageByCache(msg, TOPIC);
		}
	}

	protected void stormRule(String dt, Double storm, Double ora, Double diff)
			throws Exception {
		if (Math.abs(storm - ora) / ora > PERCENT) {
			double d = (storm - ora) / ora * 100;
			DecimalFormat df = new DecimalFormat("######.##");
			String percent = df.format(d);
			String msg = "在" + dt + " Storm标准成交净额=" + NF.format(storm)
					+ ",Oracle标准成交净额=" + NF.format(ora) + ",相差" + percent + "%";
			msg(getCache("storm-" + dt));
			if (getCache("storm-" + dt) == null) {
				sendMessageByCache(msg, TOPIC);
				putCache("storm-" + dt, "true");
			}
			return;
		}
	}

	@Override
	public String getMonitorName() {
		return "RptRealtime-monitor";
	}

	public static void main(String[] args) throws Exception {
		RptRealtimeMontrTranx m = new RptRealtimeMontrTranx();
		m.doMonitor(args);
	}

	@Override
	public void help() {
		msg("标准成交口径 销售净额监控");
		msg("\t1.当前分钟没有标准成交口径的净额，则报警");
		msg("销售净额报警规则：ABS ( Storm销售净额 – Oracle销售净额 ) / Oracle  > 9%则直接报警）");
	}
}
