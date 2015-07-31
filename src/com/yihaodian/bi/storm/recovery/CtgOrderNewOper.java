package com.yihaodian.bi.storm.recovery;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

import com.yihaodian.bi.database.DBConnection;
import com.yihaodian.bi.database.impl.OracleConnection;
import com.yihaodian.bi.hbase.dao.GosOrderDao;
import com.yihaodian.bi.hbase.dao.impl.BaseDaoImpl;
import com.yihaodian.bi.storm.common.util.ProdCtgUtil;
import com.yihaodian.bi.storm.common.util.Constant;

public class CtgOrderNewOper extends BaseDaoImpl {

	GosOrderDao dao;
	DBConnection dbcon;
	private Connection con = null;
	private PreparedStatement st = null;
	private ResultSet rs = null;

	public CtgOrderNewOper() throws IOException {
		try {
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// today yyyy-MM-dd
	public boolean ctgOrderOper(String today) {
		if (today == null || "".equals(today)) {
			return false;
		}
		Map<String, String> map = ProdCtgUtil.getProdCtgMap();
		try {
			for (String key : map.keySet()) {// 遍历所有1级类目插入数据

				List<RecoveryDataVo> list = this.getRecoveryData(key, today);
				insertRecord("bi_monitor_categ_rslt",
						Constant.COMMON_FAMILY, list);
			}
			System.out.println("品类数据修复完毕");
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(e.getClass());
			return false;
		}
		return true;
	}

	/**
	 * 批量插入 否则会报内存溢出 怀疑是多次new HTable导致
	 * 
	 * @param tableName
	 * @param columnFamily
	 * @param list
	 * @throws IOException
	 */
	public void insertRecord(String tableName, String columnFamily,
			List<RecoveryDataVo> list) throws IOException {
		int n = 1;
		HTable table = new HTable(configuration, tableName);
		List<Put> putList = new ArrayList<Put>();
		for (RecoveryDataVo rdv : list) {
			n++;
			String[] valueArr = new String[] { rdv.getColumn1(),rdv.getColumn2() };
			String[] qualifierArr = new String[] { "order_amount" ,"order_num"};
			Put put = new Put(rdv.getRowkey().getBytes());
			for (int i = 0; i < qualifierArr.length; i++) {
				String col = qualifierArr[i];
				String val = valueArr[i];
				put
						.add(columnFamily.getBytes(), col.getBytes(), val
								.getBytes());
			}
			putList.add(put);
		}
		table.put(putList);
		table.flushCommits();
		daoMsg = "插入行成功";
	}

	public List<RecoveryDataVo> getRecoveryData(String key, String today) {
		OracleConnection oc = new OracleConnection();
		List<RecoveryDataVo> list = new LinkedList<RecoveryDataVo>();
		RecoveryDataVo rdv = null;
		String sql = "select a.date_id, a.categ_lvl1_id, sum(a.ordr_amt) over(order by a.date_id, a.categ_lvl1_id) ordr_amt,sum(a.ordr_num) over(order by a.date_id, a.categ_lvl1_id) ordr_num from (SELECT to_char(CASE WHEN dpt.payment_category = 1 THEN so.order_payment_confirm_date ELSE so.order_create_time END, 'yyyymmddhh24mi') date_id, hc.categ_lvl1_id categ_lvl1_id, count(DISTINCT CASE WHEN so.parent_so_id >0 THEN  so.parent_so_id ELSE so.id END ) ordr_num,SUM(si.Order_Item_Amount) ordr_amt FROM yhd_so so INNER JOIN yhd_so_item SI ON so.id = si.order_id INNER JOIN dw.dim_payment_type dpt ON nvl(so.pay_service_type, 1) = dpt.id INNER JOIN dw.dim_prod dp on si.product_id = dp.prod_id AND dp.cur_flag = 1 INNER JOIN dw.hier_categ hc ON dp.categ_lvl_hier_skid = hc.categ_lvl_hier_skid AND hc.cur_flag = 1 WHERE si.is_item_leaf = 1 AND so.mc_site_id = 1 AND hc.categ_lvl1_id = "
				+ key
				+ " AND ((dpt.payment_category = 1 AND so.order_payment_confirm_date >= trunc(to_date('"
				+ today
				+ "', 'yyyyMMdd')) AND so.order_payment_confirm_date < trunc(to_date('"
				+ today
				+ "', 'yyyyMMdd') + 1)) OR (dpt.payment_category = 2 AND so.order_create_time >= trunc(to_date('"
				+ today
				+ "', 'yyyyMMdd')) AND so.order_create_time < trunc(to_date('"
				+ today
				+ "', 'yyyyMMdd') + 1))) GROUP BY to_char(CASE WHEN dpt.payment_category = 1 THEN so.order_payment_confirm_date ELSE so.order_create_time END, 'yyyymmddhh24mi'), hc.categ_lvl1_id ORDER BY to_char(CASE WHEN dpt.payment_category = 1 THEN so.order_payment_confirm_date ELSE so.order_create_time END, 'yyyymmddhh24mi'), hc.categ_lvl1_id) a";
		try {
			// 查询当天信息
			con = oc.getConnection();
			st = con.prepareStatement(sql);
			System.out.println("修复品类id为" + key + "的数据==" + sql);
			rs = st.executeQuery();
			while (rs.next()) {
				String date_id = rs.getString(1) + "_" + rs.getString(2);
				String ordr_amt = rs.getString(3);
				String ordr_num = rs.getString(4);
				System.out.println("date_id===" + date_id + "&&ordr_amt=="
						+ ordr_amt+ "&&ordr_num=="
						+ ordr_num);
				rdv = new RecoveryDataVo();
				rdv.setRowkey(date_id);
				rdv.setColumn1(ordr_amt);
				rdv.setColumn2(ordr_num);
				list.add(rdv);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			oc.closeConnection(rs, st, con);
		}
		return list;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println("2014-06-03".replaceAll("-", ""));
	}

}
