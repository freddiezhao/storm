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

import com.yihaodian.bi.common.util.DateUtil;
import com.yihaodian.bi.database.DBConnection;
import com.yihaodian.bi.database.impl.OracleConnection;
import com.yihaodian.bi.hbase.dao.GosOrderDao;
import com.yihaodian.bi.hbase.dao.impl.GosOrderDaoImpl;

/**
 * 北上广深销售净额 recovery 逻辑说明： step1 通过oracle表查询出相关信息并插入到临时表， step2 删除hbase
 * 昨天错误的数据，step3 将oracle生成的临时表数据插入到hbase bi_core_city_orderinfo
 * 
 * @author lining2
 */
public class SalesByCity {

	public static final String RECOVERY_TMP_TABLE = "edw1_user.recovery_sales_by_city";
	public static final SimpleDateFormat SDF_YYYY_MM_DD = new SimpleDateFormat(
			"yyyy-MM-dd");
	public static final SimpleDateFormat SDF_YYYYMMDD = new SimpleDateFormat(
			"yyyyMMdd");

	private boolean debug = true;
	GosOrderDao dao;
	DBConnection dbcon;
	private Connection con = null;

	public SalesByCity() throws IOException {
		try {
			dao = new GosOrderDaoImpl();
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
		String deleteSql = "truncate table  " + RECOVERY_TMP_TABLE;

		String flustTmpSql1 = "INSERT INTO  "
				+ RECOVERY_TMP_TABLE
				+ "SELECT dateid_regn,\r\n"
				+ "SUM(pro_amount) AS pro_amount\r\n"
				+ " FROM   (\r\n"
				+ "--网上支付\r\n"
				+ "SELECT CASE\r\n"
				+ " WHEN so.good_receiver_city_id = 1 THEN\r\n"
				+ "to_char(so.order_payment_confirm_date, 'yyyymmdd') || '_1'\r\n"
				+ "WHEN so.good_receiver_city_id = 1000 THEN\r\n"
				+ "to_char(so.order_payment_confirm_date, 'yyyymmdd') || '_2'\r\n"
				+ "WHEN so.good_receiver_city_id = 237 THEN\r\n"
				+ "to_char(so.order_payment_confirm_date, 'yyyymmdd') || '_3'\r\n"
				+ "WHEN so.good_receiver_city_id = 238 THEN\r\n"
				+ "to_char(so.order_payment_confirm_date, 'yyyymmdd') || '_4'\r\n"
				+ "ELSE\r\n"
				+ "to_char(so.order_payment_confirm_date, 'yyyymmdd') || '_5'\r\n"
				+ "END AS dateid_regn,\r\n"
				+ "SUM(si.order_item_amount - si.coupon_amount - si.promotion_amount) AS pro_amount\r\n"
				+ "FROM   stage_user.so_real so\r\n"
				+ "INNER  JOIN stage_user.so_item_real si\r\n"
				+ "ON     so.id = si.order_id\r\n"
				+ "INNER  JOIN dw.dim_payment_type dpt\r\n"
				+ "ON     so.pay_service_type = dpt.id\r\n"
				+ "WHERE  si.is_item_leaf = 1\r\n"
				+ "AND    so.is_leaf = 1\r\n"
				+ "AND    so.order_status <> 34\r\n"
				+ "AND    dpt.payment_category = 1\r\n"
				+ "AND    so.order_payment_confirm_date >= DATE'"
				+ date
				+ "')\r\n"
				+ "AND    so.order_payment_confirm_date < DATE'"
				+ date
				+ "' + 1)\r\n"
				+ "AND    NOT (so.cancel_date IS NOT NULL AND\r\n"
				+ "      trunc(so.order_payment_confirm_date) = trunc(so.cancel_date))\r\n"
				+ "GROUP  BY CASE\r\n"
				+ "          WHEN so.good_receiver_city_id = 1 THEN\r\n"
				+ "           to_char(so.order_payment_confirm_date, 'yyyymmdd') || '_1'\r\n"
				+ "          WHEN so.good_receiver_city_id = 1000 THEN\r\n"
				+ "           to_char(so.order_payment_confirm_date, 'yyyymmdd') || '_2'\r\n"
				+ "          WHEN so.good_receiver_city_id = 237 THEN\r\n"
				+ "           to_char(so.order_payment_confirm_date, 'yyyymmdd') || '_3'\r\n"
				+ "          WHEN so.good_receiver_city_id = 238 THEN\r\n"
				+ "           to_char(so.order_payment_confirm_date, 'yyyymmdd') || '_4'\r\n"
				+ "          ELSE\r\n"
				+ "           to_char(so.order_payment_confirm_date, 'yyyymmdd') || '_5'\r\n"
				+ "       END\r\n"
				+ "       UNION ALL  \r\n"
				+ "--货到付款\r\n"
				+ "SELECT CASE\r\n"
				+ "       WHEN so.good_receiver_city_id = 1 THEN\r\n"
				+ "        to_char(so.order_create_time, 'yyyymmdd') || '_1'\r\n"
				+ "       WHEN so.good_receiver_city_id = 1000 THEN\r\n"
				+ "        to_char(so.order_create_time, 'yyyymmdd') || '_2'\r\n"
				+ "       WHEN so.good_receiver_city_id = 237 THEN\r\n"
				+ "        to_char(so.order_create_time, 'yyyymmdd') || '_3'\r\n"
				+ "       WHEN so.good_receiver_city_id = 238 THEN\r\n"
				+ "        to_char(so.order_create_time, 'yyyymmdd') || '_4'\r\n"
				+ "       ELSE\r\n"
				+ "        to_char(so.order_create_time, 'yyyymmdd') || '_5'\r\n"
				+ "    END AS dateid_regn,\r\n"
				+ "    SUM(si.order_item_amount - si.coupon_amount - si.promotion_amount) AS pro_amount\r\n"
				+ "FROM   stage_user.so_real so\r\n"
				+ "INNER  JOIN stage_user.so_item_real si\r\n"
				+ "ON     so.id = si.order_id\r\n"
				+ "INNER  JOIN dw.dim_payment_type dpt\r\n"
				+ "ON     so.pay_service_type = dpt.id\r\n"
				+ "WHERE  si.is_item_leaf = 1\r\n"
				+ "AND    so.is_leaf = 1\r\n"
				+ "AND    so.order_status <> 34\r\n"
				+ "AND    dpt.payment_category = 2\r\n"
				+ "AND    so.order_create_time >= DATE'"
				+ date
				+ "'\r\n"
				+ "AND    so.order_create_time < DATE'"
				+ date
				+ "' + 1)\r\n"
				+ "AND    NOT (so.cancel_date IS NOT NULL AND\r\n"
				+ "    trunc(so.order_create_time) = trunc(so.cancel_date))\r\n"
				+ "GROUP  BY CASE\r\n"
				+ "            WHEN so.good_receiver_city_id = 1 THEN\r\n"
				+ "           to_char(so.order_create_time, 'yyyymmdd') || '_1'\r\n"
				+ "          WHEN so.good_receiver_city_id = 1000 THEN\r\n"
				+ "           to_char(so.order_create_time, 'yyyymmdd') || '_2'\r\n"
				+ "          WHEN so.good_receiver_city_id = 237 THEN\r\n"
				+ "           to_char(so.order_create_time, 'yyyymmdd') || '_3'\r\n"
				+ "            WHEN so.good_receiver_city_id = 238 THEN\r\n"
				+ "         to_char(so.order_create_time, 'yyyymmdd') || '_4'\r\n"
				+ "          ELSE\r\n"
				+ "           to_char(so.order_create_time, 'yyyymmdd') || '_5'\r\n"
				+ "       END    )\r\n" + "   GROUP  BY dateid_regn\r\n";
		try {
			boolean truncate = false;

			System.out.println(deleteSql);
			int d = s.executeUpdate(deleteSql);
			if (d == 0) {
				truncate = true;
			}
			System.out.println(deleteSql+" ok!");
			
			boolean flush1 = false;
			System.out.println(flustTmpSql1);
			int i = s.executeUpdate(flustTmpSql1);
			if (i >= 0) {
				flush1 = true;
			}
			if (truncate && flush1) {
				try {
					con.commit();
					System.out.println("INSERT INTO "+RECOVERY_TMP_TABLE+" ok!");
					return true;
				} catch (Exception e) {
					con.rollback();
					e.printStackTrace();
				} finally {
					s.close();
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return false;
	}

	/**
	 * 删除昨天hbase异常的数据
	 * 
	 * @param date
	 * @return
	 * @throws IOException
	 * @throws ParseException
	 */
	public boolean step2_dropHBaseData(String date) throws IOException,
			ParseException {
		String tableName = "bi_core_city_orderinfo";
		Date d = SDF_YYYY_MM_DD.parse(date);
		String mdate = SDF_YYYYMMDD.format(d);

		System.out.println("drop hbase table " + tableName + " date = " + date);
		List<Result> rsList = dao.getRecordByRowKeyRegex(tableName, mdate);
		for (Result result : rsList) {
			String rowKeyString = new String(result.getRow());
			System.out.println("delete - " + rowKeyString);
			if (!debug)
				dao.deleteRecord(tableName, rowKeyString);
		}
		return true;
	}

	/**
	 * 插入昨天正常的数据到HBASE
	 * 
	 * @return
	 * @throws Exception
	 */
	public boolean step3_doRecovery() throws Exception {

		String sql = "SELECT tmp.dateid_regn , " + "  tmp.pro_amount,"
				+ "FROM " + RECOVERY_TMP_TABLE + " tmp       ";

		Statement statement = null;
		ResultSet rs = null;
		try {
			statement = con.createStatement();
			System.out.println(sql);
			rs = statement.executeQuery(sql);
			while (rs.next()) {
				double order_amt = Double.valueOf(rs.getString("prod_amount"));
				String dateid_regn = rs.getString("dateid_regn");
				try {
					if (!debug)
						dao.insertGosOrder("bi_core_city_orderinfo",
								dateid_regn, "cf", "prod_amount", order_amt
										+ "");
					System.out.println("bi_core_city_orderinfo = "
							+ dateid_regn + ";order_amt=" + order_amt);

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
		}
		return true;
	}

	public static void recovery(String[] args) throws Exception {

		if (args.length > 0) {
			String date = DateUtil.transferDateToString(SDF_YYYY_MM_DD
					.parse(args[2]), DateUtil.YYYY_MM_DD_STR);
			SalesByCity recovery = new SalesByCity();
			recovery.debug = false;
			recovery.step1_createTmpTable(date);
			recovery.step2_dropHBaseData(date);
			recovery.step3_doRecovery();
		} else {
			System.out.println("Missing date params !! ");
			System.out
					.println("========================== TEST =============================");
			long zt = System.currentTimeMillis() - 86400;
			String date = SDF_YYYYMMDD.format(new Date(zt));
			SalesByCity recovery = new SalesByCity();
			recovery.debug = true;
			recovery.step1_createTmpTable(date);
			recovery.step2_dropHBaseData(date);
			recovery.step3_doRecovery();
		}
	}

	public static void main(String[] args) throws Exception {
		recovery(args);
	}
}
