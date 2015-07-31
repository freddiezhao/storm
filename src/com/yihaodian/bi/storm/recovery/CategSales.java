package com.yihaodian.bi.storm.recovery;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.hbase.client.Result;

import com.yihaodian.bi.database.DBConnection;
import com.yihaodian.bi.database.impl.OracleConnection;
import com.yihaodian.bi.hbase.dao.GosOrderDao;
import com.yihaodian.bi.hbase.dao.impl.GosOrderDaoImpl;

public class CategSales {

	public static final String RECOVERY_TMP_TABLE = "edw1_user.recovery_storm_4_mobile";
	public static final SimpleDateFormat SDF_YYYY_MM_DD = new SimpleDateFormat(
			"yyyy-MM-dd");
	public static final SimpleDateFormat SDF_YYYYMMDD = new SimpleDateFormat(
			"yyyyMMdd");

	private GosOrderDao dao;
	private DBConnection dbcon;
	private Connection con;
	private boolean step1 = false;
	private boolean step2 = false;

	public CategSales() throws IOException {
		dao = new GosOrderDaoImpl();
		dbcon = new OracleConnection();
		con = dbcon.getConnection();
	}

	public boolean step1_createTmpTable(String date, boolean onlyDebug)
			throws SQLException {
		// create tmp table
		con.setAutoCommit(false);
		Statement s = con.createStatement();
		String dropSql = "truncate table " + RECOVERY_TMP_TABLE;
		boolean drop = false;
		if (!onlyDebug) {
			try {
				int d = s.executeUpdate(dropSql);
				if (d == 0) {
					drop = true;
				}
				System.out.println(dropSql + " ok!");
			} catch (SQLException e) {
				System.out.println(dropSql + " error!");
			}
		} else {
			System.out.println("create tmp table");
			return true;
		}
		String flustTmpSql1 = "INSERT INTO "
				+ RECOVERY_TMP_TABLE
				+ "--网上支付销售数据\r\n"
				+ "SELECT /*+index(dp) parallel(8)*/ TRUNC(s.order_payment_confirm_date,'mi') AS date_time_id,\r\n"
				+ "       hc.categ_lvl1_id,\r\n"
				+ "       hc.categ_lvl1_name,       \r\n"
				+ "       SUM(soi.order_item_amount - soi.promotion_amount - soi.coupon_amount) AS ordr_amt,\r\n"
				+ "       count(distinct s.id) as ordr_num\r\n"
				+ "FROM   stage_user.so_real s\r\n"
				+ "INNER  JOIN stage_user.so_item_real soi\r\n"
				+ "ON     s.id = soi.order_id\r\n"
				+ "INNER  JOIN dw.dim_mrchnt dm\r\n"
				+ "ON     soi.merchant_id = dm.mrchnt_id\r\n"
				+ "AND    dm.cur_flag = 1\r\n"
				+ "INNER  JOIN dw.dim_prod dp\r\n"
				+ "ON     soi.product_id = dp.prod_id\r\n"
				+ "AND    dp.cur_flag = 1\r\n"
				+ "INNER  JOIN dw.hier_categ hc\r\n"
				+ "ON     dp.categ_lvl_id = hc.categ_lvl_id\r\n"
				+ "AND    hc.cur_flag = 1\r\n"
				+ "LEFT   JOIN edw1_user.yhd_payment_method pm\r\n"
				+ "ON     s.order_payment_method_id = pm.id\r\n"
				+ "LEFT   JOIN dw.dim_payment_type pt\r\n"
				+ "ON     nvl(s.pay_service_type,pm.payment_type) = pt.id\r\n"
				+ "WHERE (soi.order_item_amount <> 0 OR (soi.order_item_amount = 0 AND soi.total_integral <> 0)) "
				+ "AND pt.payment_category = 1--网上支付\r\n"
				+ "AND    (s.order_payment_confirm_date >= DATE'"
				+ date
				+ "'\r\n"
				+ "AND    s.order_payment_confirm_date <  DATE'"
				+ date
				+ "'+1)--按付款时间统计\r\n"
				+ "AND    NOT (s.cancel_date IS NOT NULL\r\n"
				+ "       AND trunc(s.order_payment_confirm_date) = trunc(s.cancel_date))--当天付款当天取消\r\n"
				+ "AND    s.is_leaf = 1\r\n"
				+ "AND    soi.is_item_leaf = 1\r\n"
				+ "GROUP  BY TRUNC(s.order_payment_confirm_date,'mi'),\r\n"
				+ "       hc.categ_lvl1_id,\r\n" + "       hc.categ_lvl1_name";
		System.out.println(flustTmpSql1);
		boolean flush1 = false;
		int f1 = s.executeUpdate(flustTmpSql1);
		if (f1 > 0) {
			flush1 = true;
		}

		String flushTmpSql2 = "INSERT INTO "
				+ RECOVERY_TMP_TABLE
				+ "--货到付款销售数据\r\n"
				+ " SELECT TRUNC(s.order_create_time,'mi') AS date_time_id,\r\n"
				+ "       hc.categ_lvl1_id,\r\n"
				+ "       hc.categ_lvl1_name,\r\n"
				+ "       SUM(soi.order_item_amount - soi.promotion_amount - soi.coupon_amount) AS ordr_amt,\r\n"
				+ "       count(distinct s.id) as ordr_num \r\n"
				+ "FROM   stage_user.so_real s\r\n"
				+ "INNER  JOIN stage_user.so_item_real soi\r\n"
				+ "ON     s.id = soi.order_id\r\n"
				+ "INNER  JOIN dw.dim_mrchnt dm\r\n"
				+ "ON     soi.merchant_id = dm.mrchnt_id\r\n"
				+ "AND    dm.cur_flag = 1\r\n"
				+ "INNER  JOIN dw.dim_prod dp\r\n"
				+ "ON     soi.product_id = dp.prod_id\r\n"
				+ "AND    dp.cur_flag = 1\r\n"
				+ "INNER  JOIN dw.hier_categ hc\r\n"
				+ "ON     dp.categ_lvl_id = hc.categ_lvl_id\r\n"
				+ "AND    hc.cur_flag = 1\r\n"
				+ "LEFT   JOIN edw1_user.yhd_payment_method pm\r\n"
				+ "ON     s.order_payment_method_id = pm.id\r\n"
				+ "LEFT   JOIN dw.dim_payment_type pt\r\n"
				+ "ON     nvl(s.pay_service_type,pm.payment_type) = pt.id\r\n"
				+ "WHERE  (soi.order_item_amount <> 0 OR (soi.order_item_amount = 0 AND soi.total_integral <> 0)) "
				+ "AND pt.payment_category = 2--货到付款\r\n"
				+ "AND    (s.order_create_time >= DATE'"
				+ date
				+ "'\r\n"
				+ "AND    s.order_create_time <  DATE'"
				+ date
				+ "'+1)--按下单时间统计\r\n"
				+ "AND    NOT (s.cancel_date IS NOT NULL\r\n"
				+ "       AND trunc(s.order_create_time) = trunc(s.cancel_date))--当天下单当天取消\r\n"
				+ "AND    s.is_leaf = 1\r\n"
				+ "AND    soi.is_item_leaf = 1\r\n"
				+ "GROUP  BY TRUNC(s.order_create_time,'mi'),\r\n"
				+ "       hc.categ_lvl1_id,\r\n" + "       hc.categ_lvl1_name";

		String flushTmpSql3 = "--净额-一级类目\r\n"
				+ "SELECT to_char(date_time_id, 'yyyyMMddhh24mi'),\r\n"
				+ "       tmp.categ_lvl1_id,\r\n"
				+ "       tmp.categ_lvl1_name,\r\n"
				+ "       SUM(ordr_amt) AS ordr_amt\r\n" + "FROM   "
				+ RECOVERY_TMP_TABLE + " tmp      \r\n"
				+ "GROUP  BY date_time_id,        \r\n"
				+ "       tmp.categ_lvl1_id,\r\n"
				+ "       tmp.categ_lvl1_name;\r\n" + "";
		System.out.println(flushTmpSql2);
		System.out.println(flushTmpSql3);
		boolean flush2 = false;
		int f2 = s.executeUpdate(flushTmpSql2);
		if (f2 > 0) {
			flush2 = true;
		}

		if (drop && flush1 && flush2) {
			try {
				con.commit();
				step1 = true;
				System.out.println("step1=" + step1);
				return true;
			} catch (Exception e) {
				con.rollback();
				e.printStackTrace();
			} finally {
				s.close();
			}
		}
		return false;
	}

	public boolean step2_dropHBaseData(String date, boolean onlyDebug)
			throws IOException, ParseException {
		String mdate = date;
		String tableName = "bi_monitor_categ_rslt";
		if (!onlyDebug) {
			// Convert Date 2015-05-01 to 20150501
			Date d = SDF_YYYY_MM_DD.parse(date);
			mdate = SDF_YYYYMMDD.format(d);
			List<Result> rsList = dao.getRecordByRowKeyRegex(tableName, mdate);
			for (Result result : rsList) {
				String rowKeyString = new String(result.getRow());
				if (!onlyDebug) {
					System.out.println("delete - " + rowKeyString);
					dao.deleteRecord(tableName, rowKeyString);
				} else {
					System.out.println("delete - " + rowKeyString);
				}
			}
			step2 = true;
		} else {
			System.out.println("drop hbase table " + tableName + " date = "+ date);
		}
		return true;
	}

	// today yyyy-MM-dd
	public boolean step3_doRecovery(String date, boolean onlyDebug)
			throws Exception {

		String sql = "SELECT m.dt,\r\n"
				+ "       m.categ_lvl1_id,\r\n"
				+ "       m.categ_lvl1_name,\r\n"
				+ "       SUM(m.ordr_amt) OVER(PARTITION BY m.categ_lvl1_id, m.categ_lvl1_name ORDER BY m.dt) AS oamt,\r\n"
				+ "       SUM(m.ordr_num) OVER(PARTITION BY m.categ_lvl1_id, m.categ_lvl1_name ORDER BY m.dt) AS onum,\r\n"
				+ "       n.dt AS last_dt\r\n" + "\r\n"
				+ "FROM   (WITH date_min AS (SELECT trunc(DATE '"
				+ date
				+ "' + (rownum - 1) / 24 / 60, 'mi') AS d_min,\r\n"
				+ "                                 0 AS id\r\n"
				+ "                          FROM   dual\r\n"
				+ "                          CONNECT BY rownum <= 1440), categ_id AS (SELECT DISTINCT x.categ_lvl1_id,\r\n"
				+ "                                                                                   x.categ_lvl1_name,\r\n"
				+ "                                                                                   0 AS id\r\n"
				+ "                                                                   FROM   "
				+ RECOVERY_TMP_TABLE
				+ " x)\r\n"
				+ "          SELECT to_char(d_min, 'yyyymmddhh24mi') AS dt,\r\n"
				+ "                 c.categ_lvl1_id,\r\n"
				+ "                 c.categ_lvl1_name,\r\n"
				+ "                 SUM(nvl(x.ordr_amt, 0)) AS ordr_amt,\r\n"
				+ "                 SUM(nvl(x.ordr_num, 0)) AS ordr_num\r\n"
				+ "          FROM   date_min dm\r\n"
				+ "          INNER  JOIN categ_id c\r\n"
				+ "          ON     dm.id = c.id\r\n"
				+ "          LEFT   OUTER JOIN (SELECT x.date_time_id,\r\n"
				+ "                                    x.categ_lvl1_id,\r\n"
				+ "                                    x.categ_lvl1_name,\r\n"
				+ "                                    SUM(ordr_amt) AS ordr_amt,\r\n"
				+ "                                    SUM(ordr_num) AS ordr_num\r\n"
				+ "                             FROM   "
				+ RECOVERY_TMP_TABLE
				+ " x\r\n"
				+ "                             GROUP  BY x.date_time_id,\r\n"
				+ "                                       x.categ_lvl1_id,\r\n"
				+ "                                       x.categ_lvl1_name) x\r\n"
				+ "          ON     dm.d_min = x.date_time_id\r\n"
				+ "          AND    c.categ_lvl1_id = x.categ_lvl1_id\r\n"
				+ "          GROUP  BY dm.d_min,\r\n"
				+ "                    c.categ_lvl1_id,\r\n"
				+ "                    c.categ_lvl1_name) m\r\n"
				+ "          \r\n"
				+ "          INNER  JOIN (SELECT to_char(MAX(date_time_id), 'yyyyMMddhh24mi') AS dt,\r\n"
				+ "                              tmp.categ_lvl1_id,\r\n"
				+ "                              tmp.categ_lvl1_name\r\n"
				+ "                       FROM   "
				+ RECOVERY_TMP_TABLE
				+ " tmp\r\n"
				+ "                       GROUP  BY tmp.categ_lvl1_id,\r\n"
				+ "                                 tmp.categ_lvl1_name\r\n"
				+ "                       ORDER  BY tmp.categ_lvl1_id) n\r\n"
				+ "          ON     m.categ_lvl1_id = n.categ_lvl1_id\r\n" + "";
		Statement statement = null;
		ResultSet rs = null;
		try {
			statement = con.createStatement();
			if (!onlyDebug)
				System.out.println(sql);
			rs = statement.executeQuery(sql);
			double ctgOrdrAmt = 0;
			double ctgOrdrNum = 0;
			while (rs.next()) {
				String todayCtgId = rs.getString("dt") + "_"+ rs.getInt("categ_lvl1_id");
				ctgOrdrAmt = Double.valueOf(rs.getString("oamt"));
				ctgOrdrNum = Double.valueOf(rs.getString("onum"));
				try {
					if (!onlyDebug) {
						dao.insertGosOrder("bi_monitor_categ_rslt", todayCtgId,"cf", new String[] { "order_num","order_amount" }, new String[] {ctgOrdrNum + "", ctgOrdrAmt + "" });
						System.out.println("bi_monitor_categ_rslt = "+ todayCtgId + ";order_amt=" + ctgOrdrAmt+ ";order_num=" + ctgOrdrNum);

						// 插入最后的last_amt
						if (rs.getString("dt").equals(rs.getString("last_dt"))) {
							String todayLastCtgId = rs.getString("dt").substring(0, 8)+ "_last_" + rs.getInt("categ_lvl1_id");
							double last_order_amt = Double.valueOf(rs.getString("oamt"));
							double last_order_num = Double.valueOf(rs.getString("onum"));
							if (!onlyDebug) {
								dao.insertGosOrder("bi_monitor_categ_rslt",todayLastCtgId, "cf", new String[] {"order_num", "order_amount" },new String[] { last_order_num + "",last_order_amt + "" });
								System.out.println("bi_monitor_categ_rslt = "+ todayLastCtgId + ";last_order_amt="+ last_order_amt + ";last_order_num="+ last_order_num);
							} else {
								System.out.println("bi_monitor_categ_rslt = "+ todayLastCtgId + ";last_order_amt="+ last_order_amt);
							}
						}

					} else {
						System.out.println("bi_monitor_categ_rslt = "+ todayCtgId + ";order_amt=" + ctgOrdrAmt);
					}

				} catch (Exception e) {
					System.out.println("write hbase error==" + e.getClass());
					e.printStackTrace();
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(e.getClass());
			return false;
		} finally {
			rs.close();
		}
		return true;
	}

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		CategSales c = new CategSales();

		System.out.println(c.step1_createTmpTable("2015-6-9", false));
		if (args.length == 0) {
			System.out.println("Warning !!!! Missing ${Date}/2015-01-01 parameter.  Test only!");
			Thread.sleep(5000);
			c.step1_createTmpTable("${date}", true);
			c.step2_dropHBaseData("${date}", true);
			c.step3_doRecovery("${date}", true);
		} else {
			SDF_YYYY_MM_DD.parse(args[0]);
			c.step1_createTmpTable(args[0], false);
			c.step2_dropHBaseData(args[0], false);
			c.step3_doRecovery(args[0], false);
		}
	}

	/**
	 * storm jar storm-0.0.1-SNAPSHOT-jar-with-dependencies.jar
	 * com.yihaodian.bi.storm.Main recovery BI_CategSales 20150609
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void recovery(String[] args) throws Exception {
		CategSales c = new CategSales();
		if (args.length == 0) {
			System.out.println("Warning !!!! Missing ${Date}/2015-01-01 parameter.  Test only!");
			Thread.sleep(5000);
			c.step1_createTmpTable("${date}", true);
			c.step2_dropHBaseData("${date}", true);
			c.step3_doRecovery("${date}", true);
		} else {
			SDF_YYYY_MM_DD.parse(args[2]);
			c.step1_createTmpTable(args[2], false);
			c.step2_dropHBaseData(args[2], false);
			c.step3_doRecovery(args[2], false);
		}
	}

}
