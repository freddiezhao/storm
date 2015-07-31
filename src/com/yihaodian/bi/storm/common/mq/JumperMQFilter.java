package com.yihaodian.bi.storm.common.mq;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.client.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yihaodian.bi.database.impl.OracleConnection;
import com.yihaodian.bi.hbase.dao.impl.BaseDaoImpl;
import com.yihaodian.bi.storm.common.model.JumpMQOrderVo;
import com.yihaodian.bi.storm.common.util.Constant;

public class JumperMQFilter {

	static final Logger logger = LoggerFactory.getLogger(JumperMQFilter.class);

	private static Connection conn = new OracleConnection().getConnection();
	//private static Map<Integer, Integer> paymentMethod = initPaymentMethod(); //
	private static Map<Integer, Integer> paymentType = initPaymentType(); //
	private static Long paymentTypeLastUpdatetime = System.currentTimeMillis();

	private static final Integer BALANCE_PAY = 0;

	private static final long TIMEOUT = 1 * 24 * 60 * 60 * 1000;

//	public static Integer getPaymentType(Long i) {
//		if (System.currentTimeMillis() - paymentTypeLastUpdatetime  > TIMEOUT) {
//			synchronized (paymentMethod) {
//				paymentMethod = initPaymentMethod();
//			}
//		}
//		
//		return paymentMethod.get(i);
//	}
	
	public static Integer getPaymentCateg(Integer i) {
//		if (System.currentTimeMillis() - paymentTypeLastUpdatetime  > TIMEOUT) {
//			synchronized (paymentType) {
//				paymentType = initPaymentType();
//			}
//		}
		
		return paymentType.get(i);
	}

	public static boolean isOnlinePay(JumpMQOrderVo jumpMQOrderVo) {
		return isSomePaymentCategory(jumpMQOrderVo, Constant.ONLINE_PAY);
	}

	public static boolean isOfflinePay(JumpMQOrderVo jumpMQOrderVo) {
		return isSomePaymentCategory(jumpMQOrderVo, Constant.OFFLINE_PAY);
	}

	public static boolean isBalancePay(JumpMQOrderVo jumpMQOrderVo) {
		return isSomePaymentType(jumpMQOrderVo, BALANCE_PAY);
	}

	private static boolean isSomePaymentType(JumpMQOrderVo jumpMQOrderVo,
			Integer paymentType) {
		Integer rt = jumpMQOrderVo.getPayServiceType();

		if (rt != null && rt == paymentType) {
			return true;
		}
		return false;
	}

	private static boolean isSomePaymentCategory(JumpMQOrderVo jumpMQOrderVo,
			Integer paymentCategFlag) {
		if (jumpMQOrderVo == null)
			throw new NullPointerException();
		Integer pt = jumpMQOrderVo.getPayServiceType();
		if (pt == null)  pt = 1;
		
//		if (pt == null || pt < 0) {// PayServiceType not exists;
//			pt = getPaymentType(jumpMQOrderVo.getOrderPaymentMethodId());
//			if (pt == null) {
//				pt = 1;
////				logger.info("Set `PaymentType` to 1 (online payment)");
//			}
//		}
		
		Integer pc = getPaymentCateg(pt);

		if (pc == null) {
			logger.error("`Payment Category` NOT found");
		} else if (pc == paymentCategFlag)
			return true;

		return false;
	}

	private static HashMap<Integer, Integer> initPaymentMethod() {
		
		logger.info("Initializing `payment method`<Map> ... ");
		HashMap<Integer, Integer> h = new HashMap<Integer, Integer>();
		try {
			Statement stmt = conn.createStatement();
			String sql = "select id, payment_type from ETL.ods_payment_method";
			ResultSet rs = stmt.executeQuery(sql);
			while (rs.next()) {
				h.put(rs.getInt(1), rs.getInt(2));
			}
		} catch (SQLException e) {
			logger.error(
					"Initializing `payment method` failed. Error occured when querying `ETL.ods_payment_method`.",
					e);
		}
		logger.info("Successfully initialized `payment method`<Map>");

		return h;
	}

	private static HashMap<Integer, Integer> initPaymentType() {

		logger.info("Initializing `payment type`<Map> ... ");
		HashMap<Integer, Integer> h = new HashMap<Integer, Integer>();
		try {
			Statement stmt = conn.createStatement();
			String sql = "select id, payment_category from ETL.dim_payment_type";
			ResultSet rs = stmt.executeQuery(sql);
			while (rs.next()) {
				h.put(rs.getInt(1), rs.getInt(2));
			}
		} catch (SQLException e) {
			logger.error(
					"Initializing `Payment category` failed. Error occured when querying `ETL.dim_payment_type`.",
					e);
		}
		logger.info("Successfully initialized `payment type`<Map>");
		
		return h;
	}

	public static void main(String[] args) {
	}

}
