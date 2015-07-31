package com.yihaodian.bi.database.impl;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.yihaodian.bi.database.DBConnection;
import com.yihaodian.bi.storm.common.model.FunnelVo;
import com.yihaodian.bi.storm.common.model.HotWordsVo;

public class OracleConnection implements DBConnection {

	public OracleConnection() {
	}

	private final String DRIVER = "oracle.jdbc.driver.OracleDriver";

	// private final String URL =
	// "jdbc:oracle:thin:@10.0.1.222:1521/edwstd01";// 本环境地
	private final String URL = "jdbc:oracle:thin:@ex4-dw1.int.yihaodian.com:1521/edwstd01";// 线上环境

	private final String USERNAME = "edw1_user";

	private final String PASSWORD = "a2YtgCbso6V";

	private final String SQL_FUNNEL_DATA = "select t.count_hour,t.count_minut,t.qz_sec_vstrs,t.detl_sec_vstrs,t.cart_vstrs,t.ordr_end_vstrs,t.ordr_num from dw.rpt_trfc_minut t where to_number(to_char(date_id,'yyyymmdd')||count_hour||decode(count_minut,'30','30','00')) = (select max(to_number(to_char(date_id,'yyyymmdd')||count_hour||decode(count_minut,'30','30','00'))) from dw.rpt_trfc_kw_minut)";

	private final String SQL_LAST_WEEK_FUNNEL_DATA = "select t.qz_sec_vstrs,t.detl_sec_vstrs,t.cart_vstrs,t.ordr_end_vstrs from DW.RPT_TRFC_MINUT t where t.date_id = trunc(sysdate) - 1 and t.count_hour = ? and t.count_minut = ?";

	private final String SQL_HOT_WORDS = "select * from (select a.count_hour, a.count_minut, a.page_value, round(a.kw_sec_vstrs/3) kw_sec_vstrs, a.kw_rate * 100 as kw_rate, row_number() over(order by a.nm) order_num from (select t.count_hour, t.count_minut, t.page_value, t.kw_sec_vstrs, round(t.kw_detl_vstrs / t.kw_sec_vstrs, 2) kw_rate, row_number() over(order by t.kw_sec_vstrs desc) nm from dw.rpt_trfc_kw_minut t where to_number(to_char(date_id,'yyyymmdd')||count_hour||decode(count_minut,'30','30','00')) = (select max(to_number(to_char(date_id,'yyyymmdd')||count_hour||decode(count_minut,'30','30','00'))) from dw.rpt_trfc_kw_minut)) a where a.nm <= 1200 and mod(nm, 12) = ? order by ceil(dbms_random.value(0, 100))) b where b.page_value is not null and b.kw_rate is not null";

	public Connection getConnection() {
		Connection conn = null;
		try {
			Class.forName(DRIVER);
			conn = DriverManager.getConnection(URL, USERNAME, PASSWORD);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return conn;
	}

	public void closeConnection(ResultSet set, PreparedStatement pre,
			Connection conn) {
		try {
			if (set != null) {
				set.close();
			}
			if (pre != null) {
				pre.close();
			}
			if (conn != null) {
				conn.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 查询漏斗数据
	 * 
	 * @return
	 */
	public FunnelVo findFunnel() {
		Connection conn = null;
		PreparedStatement st = null;
		ResultSet rs = null;
		FunnelVo fv = null;
		try {
			// 查询当天信息
			conn = this.getConnection();
			st = conn.prepareStatement(SQL_FUNNEL_DATA);
			rs = st.executeQuery();
			while (rs.next()) {
				fv = new FunnelVo();
				fv.setHour(rs.getInt(1));
				fv.setMin(rs.getInt(2));
				fv.setQz_sec_vstrs(rs.getLong(3));
				fv.setDetl_sec_vstrs(rs.getLong(4));
				fv.setCart_vstrs(rs.getLong(5));
				fv.setOrdr_end_vstrs(rs.getLong(6));
				fv.setOrdr_num(rs.getLong(7));
				// 计算占比
				Double detlPerct = 0.0;
				Double cartPerct = 0.0;
				Double ordrEndPerct = 0.0;
				if (fv.getQz_sec_vstrs() != 0) {
					detlPerct = (double) fv.getDetl_sec_vstrs()
							/ fv.getQz_sec_vstrs();
					cartPerct = (double) fv.getCart_vstrs()
							/ fv.getQz_sec_vstrs();
					ordrEndPerct = (double) fv.getOrdr_end_vstrs()
							/ fv.getQz_sec_vstrs();
				}
				fv.setDetl_perct(String.valueOf((Math.round(detlPerct * 100))));
				fv.setCart_perct(String.valueOf((Math.round(cartPerct * 100))));
				fv.setOrdr_end_perct(String.valueOf((Math
						.round(ordrEndPerct * 100))));

				// 计算过程转化比例
				if (fv.getQz_sec_vstrs() != 0) {
					Double qzTransf = (double) fv.getOrdr_num()
							/ fv.getQz_sec_vstrs();
					Double detlTransf = (double) fv.getDetl_sec_vstrs()
							/ fv.getQz_sec_vstrs();
					fv.setDetl_transf(String.valueOf((Math
							.round(detlTransf * 100))));
					fv.setQz_transf(String
							.valueOf((Math.round(qzTransf * 100))));
				} else {
					fv.setDetl_transf("-");
				}
				if (fv.getDetl_sec_vstrs() != 0) {
					Double cartTransf = (double) fv.getCart_vstrs()
							/ fv.getDetl_sec_vstrs();
					fv.setCart_transf(String.valueOf((Math
							.round(cartTransf * 100))));
				} else {
					fv.setCart_transf("-");
				}
				if (fv.getCart_vstrs() != 0) {
					Double ordrEndTransf = (double) fv.getOrdr_end_vstrs()
							/ fv.getCart_vstrs();
					fv.setOrdr_end_transf(String.valueOf((Math
							.round(ordrEndTransf * 100))));
				} else {
					fv.setOrdr_end_transf("-");
				}
			}

			// 查询上周同期信息
			st = conn.prepareStatement(SQL_LAST_WEEK_FUNNEL_DATA);
			st.setInt(1, fv.getHour()); // 这里将问号赋值
			st.setInt(2, fv.getMin()); // 这里将问号赋值
			rs = st.executeQuery();
			while (rs.next()) {
				// 计算上周同期比较增幅
				if (rs.getLong(1) != 0) {
					Double d1 = (double) fv.getQz_sec_vstrs() / rs.getLong(1);
					if (rs.getLong(1) >= fv.getQz_sec_vstrs()) {
						fv.setLw_qz_sec_perct("-"
								+ String.valueOf(100 - (Math.round(d1 * 100))));
					} else {
						fv.setLw_qz_sec_perct("+"
								+ String.valueOf((Math.round(d1 * 100) - 100)));
					}
				} else {
					fv.setLw_qz_sec_perct("-");
				}
				if (rs.getLong(2) != 0) {
					Double d2 = (double) fv.getDetl_sec_vstrs() / rs.getLong(2);
					if (rs.getLong(2) >= fv.getDetl_sec_vstrs()) {
						fv.setLw_detl_sec_perct("-"
								+ String.valueOf(100 - (Math.round(d2 * 100))));
					} else {
						fv.setLw_detl_sec_perct("+"
								+ String.valueOf((Math.round(d2 * 100) - 100)));
					}
				} else {
					fv.setLw_detl_sec_perct("-");
				}
				if (rs.getLong(3) != 0) {
					Double d3 = (double) fv.getCart_vstrs() / rs.getLong(3);
					if (rs.getLong(3) >= fv.getCart_vstrs()) {
						fv.setLw_cart_perct("-"
								+ String.valueOf(100 - (Math.round(d3 * 100))));
					} else {
						fv.setLw_cart_perct("+"
								+ String.valueOf((Math.round(d3 * 100) - 100)));
					}
				} else {
					fv.setLw_cart_perct("-");
				}
				if (rs.getLong(4) != 0) {
					Double d4 = (double) fv.getOrdr_end_vstrs() / rs.getLong(4);
					if (rs.getLong(4) >= fv.getOrdr_end_vstrs()) {
						fv.setLw_ordr_end_perct("-"
								+ String.valueOf(100 - (Math.round(d4 * 100))));
					} else {
						fv.setLw_ordr_end_perct("+"
								+ String.valueOf((Math.round(d4 * 100) - 100)));
					}
				} else {
					fv.setLw_ordr_end_perct("-");
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			this.closeConnection(rs, st, conn);
		}
		return fv;
	}

	/**
	 * 查询热词搜索
	 * 
	 * @param showBatchNum
	 * @return
	 */
	public List<HotWordsVo> findHotWords(int showBatchNum) {
		List<HotWordsVo> list = new ArrayList<HotWordsVo>();
		Connection conn = null;
		PreparedStatement st = null;
		ResultSet rs = null;
		HotWordsVo hw = null;
		try {
			// 查询当天信息
			conn = this.getConnection();
			st = conn.prepareStatement(SQL_HOT_WORDS);
			st.setInt(1, showBatchNum); // 这里将问号赋值
			rs = st.executeQuery();
			while (rs.next()) {
				hw = new HotWordsVo();
				hw.setHour(rs.getInt(1));
				hw.setMin(rs.getInt(2));
				hw.setPage_value(rs.getString(3));
				hw.setKw_sec_vstrs(rs.getLong(4));
				hw.setKw_rate(rs.getInt(5));
				hw.setOrder_num(rs.getInt(6));
				list.add(hw);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			this.closeConnection(rs, st, conn);
		}
		return list;
	}

}
