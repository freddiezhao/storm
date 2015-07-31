package com.yihaodian.bi.storm.recovery;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.commons.lang.StringUtils;

import com.yihaodian.bi.common.util.DateUtil;
import com.yihaodian.bi.database.DBConnection;
import com.yihaodian.bi.database.impl.OracleConnection;
import com.yihaodian.bi.storm.monitor.RptRealtimeMontrCancl;

/**
 * 成交订单净额明细（粒度到产品ID,二级类目，品牌ID等）recovery 逻辑说明： step1 将正确的数据插入到临时表oracle， step2 删除
 * 昨天错误的数据，再插入正确的数据到oracle
 * 
 * @author lining2
 * 
 */
public class RptRealtimeMontrTranx {
	public static final String RECOVERY_TMP_TABLE = "edw1_user.recovery_rptRealtimeMontrTranx";
	public static final SimpleDateFormat SDF_YYYY_MM_DD = new SimpleDateFormat(
			"yyyy-MM-dd");
	private DBConnection dbcon;
	private Connection con;

	public RptRealtimeMontrTranx() throws IOException {
		try {
			dbcon = new OracleConnection();
			con = dbcon.getConnection();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 创建昨天正常的数据到临时表
	 * 
	 * @param date
	 * @return
	 * @throws SQLException
	 */
	public boolean step1_createTmpTable(String date) throws SQLException {
		con.setAutoCommit(false);
		Statement s = con.createStatement();
		String truncateSql = "truncate table  " + RECOVERY_TMP_TABLE;
		boolean drop = false;
		try {
			int truncate = s.executeUpdate(truncateSql);
			if (truncate == 0) {
				drop = true;
			}
			System.out.println(drop + " " + truncateSql + " ok!");
		} catch (SQLException e) {
			e.printStackTrace();
		}
		System.out.println(truncateSql);
		String flustTmpSql1 = "INSERT INTO  "
				+ RECOVERY_TMP_TABLE
				+ "--网上支付销售数据\r\n"
				+ " SELECT /*+index(t4) parallel(8)*/trunc(t1.order_payment_confirm_date) AS date_id,\r\n"
				+ " trunc(t1.order_payment_confirm_date,'mi') AS date_time_id,\r\n"
				+ " t1.good_receiver_city_id AS city_id,\r\n"
				+ " t1.order_source AS ordr_srce,\r\n"
				+ " t1.business_type AS sale_biz_type,\r\n "
				+ " t1.warehouse_id AS whse_id,\r\n"
				+ " t3.biz_unit, t6.categ_lvl1_id, t6.categ_lvl2_id,t5.mg_brand_id, t2.product_id AS prod_id,t1.end_user_id,\r\n"
				+ " CASE WHEN t1.bought_times = 1 THEN 1 ELSE 0 END new_cust_flag,\r\n"
				+ " CASE WHEN t1.parent_so_id > 0 THEN t1.parent_so_id ELSE  t1.id END parnt_ordr_id,\r\n"
				+ " t2.order_item_amount - t2.promotion_amount - t2.coupon_amount -ratio_to_report(t2.order_item_amount) OVER(PARTITION BY t2.order_id) * t1.order_paid_by_rebate AS pm_net_amt,"
				+ " t1.update_time AS updt_time,\r\n"
				+ "  t1.id as ordr_id\r\n"
				+ "FROM   stage_user.so_real t1\r\n"
				+ "INNER  JOIN stage_user.so_item_real t2\r\n"
				+ "ON     t1.id = t2.order_id\r\n"
				+ "INNER  JOIN dw.dim_mrchnt t3\r\n"
				+ "ON     t2.merchant_id = t3.mrchnt_id\r\n"
				+ "AND    t3.cur_flag = 1\r\n"
				+ "INNER  JOIN dw.dim_prod t4\r\n"
				+ "ON     t2.product_id = t4.prod_id\r\n"
				+ "AND    t4.cur_flag = 1\r\n"
				+ "INNER  JOIN dw.hier_categ t6\r\n"
				+ "ON     t4.categ_lvl_id = t6.categ_lvl_id\r\n"
				+ "AND    t6.cur_flag = 1\r\n"
				+ "INNER  JOIN dw.dim_brand t5\r\n"
				+ "ON t4.brand_id = t5.brand_id\r\n"
				+ " AND    t5.cur_flag = 1\r\n"
				+ "INNER   JOIN dw.dim_payment_type t9\r\n"
				+ "ON     t1.pay_service_type = t9.id\r\n"
				+ "WHERE  t9.payment_category = 1--网上支付\r\n"
				+ "AND    (t1.order_payment_confirm_date >= DATE'"
				+ date
				+ "'\r\n"
				+ "AND    t1.order_payment_confirm_date <  DATE'"
				+ date
				+ "'+1)--按付款时间统计\r\n"
				+ "AND    NOT (t1.cancel_date IS NOT NULL\r\n"
				+ "       AND trunc(t1.order_payment_confirm_date) = trunc(t1.cancel_date))--当天付款当天取消\r\n"
				+ "AND    t1.is_leaf = 1\r\n"
				+ "AND    t2.is_item_leaf = 1   "
				+ " AND    t4.prod_type NOT IN (4, 7) -- 剔除礼品卡\r\n"
				+ " AND   (t2.order_item_amount <> 0 OR (t2.order_item_amount = 0 AND t2.total_integral <> 0)) -- 剔除0元非积分兑换商品\r\n";
		boolean flush1 = false;

		if (drop) {
			int f1 = s.executeUpdate(flustTmpSql1);
			if (f1 > 0)
				flush1 = true;
		}

		System.out.println(flustTmpSql1);
		String flushTmpSql2 = "INSERT INTO "
				+ RECOVERY_TMP_TABLE
				+ "--货到付款销售数据\r\n"
				+ " SELECT /*+index(t4) use_nl(t1 t2) parallel(8)*/ trunc(t1.order_create_time) AS date_id,\r\n"
				+ " trunc(t1.order_create_time, 'mi') AS date_time_id,\r\n"
				+ " t1.good_receiver_city_id AS city_id,\r\n"
				+ " t1.order_source AS ordr_srce,\r\n"
				+ " t1.business_type AS sale_biz_type,\r\n "
				+ " t1.warehouse_id AS whse_id,\r\n"
				+ " t3.biz_unit, t6.categ_lvl1_id, t6.categ_lvl2_id,t5.mg_brand_id, t2.product_id AS prod_id,t1.end_user_id,\r\n"
				+ " CASE WHEN t1.bought_times = 1 THEN 1 ELSE 0 END new_cust_flag,\r\n"
				+ " CASE WHEN t1.parent_so_id > 0 THEN t1.parent_so_id ELSE  t1.id END parnt_ordr_id,\r\n"
				+ " t2.order_item_amount - t2.promotion_amount - t2.coupon_amount -ratio_to_report(t2.order_item_amount) OVER(PARTITION BY t2.order_id) * t1.order_paid_by_rebate AS pm_net_amt,"
				+ " t1.update_time AS updt_time,\r\n"
				+ "  t1.id as ordr_id\r\n"
				+ "FROM   stage_user.so_real t1\r\n"
				+ "INNER  JOIN stage_user.so_item_real t2\r\n"
				+ "ON     t1.id = t2.order_id\r\n"
				+ "INNER  JOIN dw.dim_mrchnt t3\r\n"
				+ "ON     t2.merchant_id = t3.mrchnt_id\r\n"
				+ "AND    t3.cur_flag = 1\r\n"
				+ "INNER  JOIN dw.dim_prod t4\r\n"
				+ "ON     t2.product_id = t4.prod_id\r\n"
				+ "AND    t4.cur_flag = 1\r\n"
				+ "INNER  JOIN dw.hier_categ t6\r\n"
				+ "ON     t4.categ_lvl_id = t6.categ_lvl_id\r\n"
				+ "AND    t6.cur_flag = 1\r\n"
				+ "INNER  JOIN dw.dim_brand t5\r\n"
				+ "ON t4.brand_id = t5.brand_id\r\n"
				+ " AND    t5.cur_flag = 1\r\n"
				+ "INNER   JOIN dw.dim_payment_type t9\r\n"
				+ "ON     t1.pay_service_type = t9.id\r\n"
				+ "WHERE  t9.payment_category = 2--货到付款\r\n"
				+ "AND    (t1.order_create_time >= DATE'"
				+ date
				+ "'\r\n"
				+ "AND    t1.order_create_time <  DATE'"
				+ date
				+ "'+1)--按下单时间统计\r\n"
				+ "AND    NOT (t1.cancel_date IS NOT NULL\r\n"
				+ "       AND trunc(t1.order_create_time) = trunc(t1.cancel_date))--当天下单当天取消\r\n"
				+ "AND    t1.is_leaf = 1\r\n"
				+ "AND    t2.is_item_leaf = 1  "
				+ "AND    t4.prod_type NOT IN (4, 7) -- 剔除礼品卡\r\n"
				+ "AND    (t2.order_item_amount <> 0 OR (t2.order_item_amount = 0 AND t2.total_integral <> 0)) -- 剔除0元非积分兑换商品\r\n";
		boolean flush2 = false;
		if (drop) {
			int f2 = s.executeUpdate(flushTmpSql2);
			if (f2 > 0)
				flush2 = true;
		}
		System.out.println(flushTmpSql2);
		if (drop && flush1 && flush2) {
			try {
				con.commit();
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

	/**
	 * 先删除昨天异常的数据，然后插入昨天正常的数据到oracle
	 * 
	 * @param date
	 * @return
	 * @throws SQLException
	 */
	public boolean step2_doRecovery(String date) throws SQLException {

		String deleteErrorSql = "delete from  rpt.rpt_realtime_montr_tranx_s  where  date_id=DATE'"
				+ date + "'";
		String recoverySql = "insert into   rpt.rpt_realtime_montr_tranx_s  select  *  from "
				+ RECOVERY_TMP_TABLE + " ";
		System.out.println(deleteErrorSql);
		System.out.println(recoverySql);
		boolean delete = false;
		boolean insert = false;

		con.setAutoCommit(false);
		Statement s = con.createStatement();
		int d = s.executeUpdate(deleteErrorSql);
		int i = s.executeUpdate(recoverySql);
		if (d >= 0) {
			delete = true;
		}
		if (i > 0) {
			insert = true;
		}

		if (delete && insert) {
			try {
				con.commit();
				System.out
						.println("rpt.rpt_realtime_montr_tranx_s recovery  ok!");
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

	public static void recovery(String[] args) throws IOException,
			SQLException, ParseException {
		if (args == null || args.length < 3 || StringUtils.isEmpty(args[0])) {
			System.out.println("Missing params：date or recovery or gpName");
		}
		if (args.length == 3) {
			String date = DateUtil.transferDateToString(SDF_YYYY_MM_DD
					.parse(args[2]), DateUtil.YYYY_MM_DD_STR);
			RptRealtimeMontrTranx recovery = new RptRealtimeMontrTranx();
			recovery.step1_createTmpTable(date);
			recovery.step2_doRecovery(date);
		} else {
			System.out.println("Missing date params");
		}
	}

	public static void main(String[] args) throws Exception {
		RptRealtimeMontrTranx m = new RptRealtimeMontrTranx();
		m.recovery(args);
	}
}
