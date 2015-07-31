package com.yihaodian.bi.storm.monitor;

import java.sql.ResultSet;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;

import com.yihaodian.bi.common.util.DateUtil;
import com.yihaodian.bi.hbase.dao.GosOrderDao;
import com.yihaodian.bi.hbase.dao.impl.GosOrderDaoImpl;

/**
 * 监控 实时成交净额、移动实时成交净额 、全站分钟顾客数，移动分钟顾客数
 * 
 * @author lining2
 * 
 */
public class CustomerAndSales extends BaseMonitor {
	protected GosOrderDao hbaseOrderDao = null;

	public static final String TOPIC = "[STORM监控][监控室大屏]";
	// 全站成交净额
	public static final String STORM_AMT_TABLE = "bi_monitor_order_info_rslt";// HBASE表
	// 移动成交净额
	public static final String STORM_MOBILE_AMT_TABLE = "bi_monitor_mobile_order_rslt";// HBASE表
	// 全站顾客数
	public static final String STORM_CUSTOMER_TABLE = "bi_monitor_customer_rslt";// HBASE表
	// 移动顾客数
	public static final String STORM_MOBILE_CUSTOMER_TABLE = "bi_monitor_mobile_customer_rslt";// HBASE表

	// 实时净额包含(全站，移动)大屏oracle
	public static final String ORACLE_AMT_TABLE = "rpt.rpt_realtime_mp_amt";
	// 实时顾客数包含(全站，移动)大屏oracle
	public static final String ORACLE_CUSTOMER_TABLE = "rpt.rpt_realtime_mp_user";

	private static double PERCENT_AMT = 0.09D;
	private static NumberFormat NF = NumberFormat.getInstance();

	public void monitor(String[] args) throws Exception {

		String curDate = DateUtil.transferDateToString(DateUtil.getDate(),
				DateUtil.YYYYMMDD_STR);
		if (args.length >= 1) {
			PERCENT_AMT = Double.parseDouble(args[0]);
		}
		if (args.length >= 2) {
			curDate = args[1];
		}
		// 监控全站顾客数
		monitorCustomer(STORM_CUSTOMER_TABLE, curDate, "all_customer");
		// 监控移动顾客数
		monitorCustomer(STORM_MOBILE_CUSTOMER_TABLE, curDate, "mobile_customer");
		
		// 监控全站成交净额
		monitorSalesAmt(STORM_AMT_TABLE, curDate, "all_salesAmt");
		// 监控移动成交净额
		monitorSalesAmt(STORM_MOBILE_AMT_TABLE, curDate, "mobile_salesAmt");
	}

	protected void monitorCustomer(String table, String curDate, String type)
			throws Exception {
		SimpleDateFormat sdf = new SimpleDateFormat(DateUtil.HHMM);
		long time0 = DateUtil.getDate().getTime() - 4 * 60 * 1000;
		long time1 = DateUtil.getDate().getTime() - 5 * 60 * 1000;
		long time2 = DateUtil.getDate().getTime() - 6 * 60 * 1000;
		
		Date date0 = new Date(time0);
		Date date1 = new Date(time1);
		Date date2 = new Date(time2);
		String curMin0 = sdf.format(date0);// 分钟
		String curMin1 = sdf.format(date1);// 分钟
		String curMin2 = sdf.format(date2);// 分钟

		String topic = "";
		if ("all_customer".equals(type)) {
			topic = TOPIC + "[实时顾客数(全站)]";
		} else if ("mobile_customer".equals(type)) {
			topic = TOPIC + "[实时顾客数(移动)]";
		}

		double v_customer0 = getStormValueNew(table, curDate, curMin0, type);
		double v_customer1 = getStormValueNew(table, curDate, curMin1, type);
		double v_customer2 = getStormValueNew(table, curDate, curMin2, type);
		if (v_customer0 == 0 && v_customer1==0 && v_customer2==0) {
			String msg = "在" +curDate+" " +curMin0 + "最近3分钟内没有顾客数！";
			sendMessageByCache(msg, topic);
		} 

	}

	protected void monitorSalesAmt(String table, String curDate, String type)
			throws Exception {
		SimpleDateFormat sdf = new SimpleDateFormat(DateUtil.YYYYMMDDHHMM_STR);
		long time = DateUtil.getDate().getTime() - 3 * 60 * 1000;
		Date date1 = new Date(time);
		String curMin = sdf.format(date1).substring(8, 12);// 分钟
		String dt = DateUtil.transferDateToString(date1,
				DateUtil.YYYY_MM_DD_HH_MM_STR);
		String topic = "";
		if ("all_salesAmt".equals(type)) {
			topic = TOPIC + "[实时成交净额(全站)]";
		} else if ("mobile_salesAmt".equals(type)) {
			topic = TOPIC + "[实时成交净额(移动)]";
		}
		double v_amt = getStormValueNew(table, curDate, curMin, type);
		if (v_amt == 0) {
			String msg = "在"+dt+"当前分钟没有成交净额数据！";
			sendMessageByCache(msg, topic);
		} else if (v_amt > 0) {
			msg("table="+table+"curDate="+curDate+"curMin="+curMin+"type="+type);
			double oracle = getOracleValue(table, curDate, curMin, type);
			msg("oracle="+oracle);
			stormSalesAmtRule(dt, v_amt, oracle, topic);
		}

	}

	protected void stormSalesAmtRule(String dt, Double storm, Double oracle,
			 String topic) throws Exception {
		if (Math.abs(storm  - oracle) / oracle > PERCENT_AMT) {
			double d = (storm  - oracle) / oracle * 100;
			DecimalFormat df = new DecimalFormat("######.##");
			String percent = df.format(d);
			String msg = "在" + dt + " Storm成交净额="
					+ NF.format(storm) + ",Oracle成交净额="
					+ NF.format(oracle) + ",相差" + percent + "%";
			msg(msg);
			sendMessageByCache(msg, topic);
			return;
		}
	}


	private double getOracleValue(String table, String curDate, String curMin,
			String type) throws Exception {

		Statement st = oracleCon.createStatement();
		String date = curDate + " " + curMin + "00";
		double d_value = 0.0;

		String str = " ";

		if ("all_salesAmt".equals(type) || "all_customer".equals(type)) {
			str = " ";
		} else if ("mobile_salesAmt".equals(type)
				|| "mobile_customer".equals(type)) {
			str = " pltfm_lvl1_id = 100 AND ";
		}

		if (type.contains("Amt")) {
			String sql = "SELECT SUM(t.ordr_amt) AS ordr_amt\r\n" + "FROM   "
					+ ORACLE_AMT_TABLE + " t\r\n" + "WHERE " + str
					+ " t.date_time_id = to_date('" + date
					+ "', 'YYYYMMDD HH24MISS')";
			msg(sql);
			ResultSet rs1 = st.executeQuery(sql);
			while (rs1.next()) {
				d_value = rs1.getDouble(1);
			}
		} else if (type.contains("customer")) {
			String cusSql = "SELECT \r\n"
					+ "       COUNT(DISTINCT CASE\r\n"
					+ "                WHEN t.date_time_id >= trunc(SYSDATE)\r\n"
					+ "                     AND t.date_time_id < trunc(SYSDATE) + 1 THEN\r\n"
					+ "                 t.end_user_id\r\n"
					+ "                ELSE\r\n" + "                 NULL\r\n"
					+ "             END)\r\n" + "FROM   "
					+ ORACLE_CUSTOMER_TABLE + " t\r\n" + "WHERE " + str
					+ "t.date_time_id = to_date('" + date
					+ "', 'YYYYMMDD HH24MISS')";

			msg(cusSql);
			ResultSet rs = st.executeQuery(cusSql);
			while (rs.next()) {
				d_value = rs.getDouble(1);
			}
		}

		return d_value;
	}

	private double getStormValueNew(String table, String curDate,
			String curMin, String type) throws Exception {
		String xTitleValue = curMin.substring(0, 2) + "."
				+ curMin.substring(2, 4);
		double orderAmountValue = 0;
		double d_value = 0;
		String min_value = "";
		String rowkey = curDate + "_amount";
		String cusRowkey = curDate + curMin;
		String[] cols = new String[2];
		cols[0] = "order_amount"; 
		cols[1] = "xTitle";
		hbaseOrderDao = new GosOrderDaoImpl();
		List<Result> rsList = null;
		boolean flag =false ;
		if ("all_salesAmt".equals(type) || "mobile_salesAmt".equals(type)) {
			rsList = hbaseOrderDao.getRecordByRowKeyRegex(table, rowkey, cols);
			for (Result result : rsList) {
				if(flag) {
					break;
				}
				String rowKeyString = new String(result.getRow());
				for (KeyValue keyValue : result.raw()) {
					if ("order_amount".equals(new String(keyValue.getQualifier()))) {
						orderAmountValue = Double.parseDouble(new String(keyValue.getValue()));
					}

					if ("xTitle".equals(new String(keyValue.getQualifier()))) {
						min_value = new String(keyValue.getValue());
					}
//					msg("min_value=" + min_value);
					if (min_value.equals(xTitleValue)) {
						d_value = orderAmountValue;
						flag=true;
					}
					if(flag) {
						break;
					}
					
				}
			}
		} else if ("all_customer".equals(type)|| "mobile_customer".equals(type)) {
			msg("cusRowkey="+cusRowkey+",table="+table);
			String value = getHBase().getOrderDao().getValueByCol(table,cusRowkey, "customer_num");
			if (value==null||"".equals(value)) {
				d_value = 0.0;
			}else {
				d_value = Double.valueOf(value);
			}

		}
		msg("d_value=" + d_value);
		return d_value;
	}
	public String getMonitorName() {
		return "顾客数和销售净额监控";
	}

	public void help() {
		msg("顾客数和销售净额监控");
		msg("\t1. 依次检查当前时间的全站实时成交净额、移动实时成交净额是否为空，如果为空则报警");
		msg("\t2. 依次检查当前时间的全站分钟顾客数和移动分钟顾客数是否为空，如果为空则报警");
		msg("销售净额报警规则：ABS ( Storm销售净额 - Storm取消金额 – Oracle销售净额 ) / Oracle  > 9%则直接报警）");
		msg("顾客数报警规则： 当前时间顾客数为0就报警");

	}
	public static void main(String[] args) throws Exception {
		CustomerAndSales m = new CustomerAndSales();
	    m.doMonitor(args);
	}

}
