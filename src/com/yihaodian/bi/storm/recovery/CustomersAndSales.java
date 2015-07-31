package com.yihaodian.bi.storm.recovery;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

import com.yihaodian.bi.database.DBConnection;
import com.yihaodian.bi.database.impl.OracleConnection;
import com.yihaodian.bi.hbase.dao.GosOrderDao;
import com.yihaodian.bi.hbase.dao.impl.BaseDaoImpl;
import com.yihaodian.bi.storm.common.util.CommonUtil;
import com.yihaodian.bi.storm.common.util.Constant;

public class CustomersAndSales extends BaseDaoImpl{
	private String SQL_RECOVER_DATA_LINE;
	private String SQL_RECOVER_DATA_COL;
	private String date;
	GosOrderDao dao;
	DBConnection dbcon;
	private Connection con = null;
	private PreparedStatement st = null;
	private ResultSet rs = null;
	private String tableName;
	private String rowKeyRegexLine;// 线图rowkey
	private String rowKeyRegexCol;// 柱状图rowkey

	public CustomersAndSales() throws IOException {
		super();
	}
	
	public CustomersAndSales(String date) throws IOException{
		if (date == null || "".equals(date)) {
			 this.date=new SimpleDateFormat("yyyyMMdd").format(Calendar.getInstance().getTime());
		}else {		
			this.date = date;
			}

		SQL_RECOVER_DATA_LINE = "select a.date_id, sum(a.ordr_amt) over(order by a.date_id) ordr_amt from (SELECT to_char(CASE WHEN dpt.payment_category = 1 THEN so.order_payment_confirm_date ELSE so.order_create_time END, 'yyyymmdd hh24mi') date_id, SUM(si.Order_Item_Amount) ordr_amt FROM stage_user.so_real so INNER JOIN stage_user.so_item_real SI ON so.id = si.order_id INNER JOIN dw.dim_payment_type dpt ON nvl(so.pay_service_type, 1) = dpt.id WHERE si.is_item_leaf = 1 AND so.mc_site_id = 1 AND ((dpt.payment_category = 1 AND so.order_payment_confirm_date >= trunc(to_date('"
				+ date
				+ "','yyyyMMdd')) AND so.order_payment_confirm_date < trunc(to_date('"
				+ date
				+ "','yyyyMMdd')+1)) OR (dpt.payment_category = 2 AND so.order_create_time >= trunc(to_date('"
				+ date
				+ "','yyyyMMdd')) AND so.order_create_time < trunc(to_date('"
				+ date
				+ "','yyyyMMdd')+1))) GROUP BY to_char(CASE WHEN dpt.payment_category = 1 THEN so.order_payment_confirm_date ELSE so.order_create_time END, 'yyyymmdd hh24mi') ORDER BY to_char(CASE WHEN dpt.payment_category = 1 THEN so.order_payment_confirm_date ELSE so.order_create_time END, 'yyyymmdd hh24mi')) a";

		SQL_RECOVER_DATA_COL = "select to_number(a.date_id) + 1 hour, a.ordr_amt from (SELECT to_char(CASE WHEN dpt.payment_category = 1 THEN so.order_payment_confirm_date ELSE so.order_create_time END, 'hh24') date_id, SUM(si.Order_Item_Amount) ordr_amt FROM stage_user.so_real so INNER JOIN stage_user.so_item_real SI ON so.id = si.order_id INNER JOIN dw.dim_payment_type dpt ON nvl(so.pay_service_type, 1) = dpt.id WHERE si.is_item_leaf = 1 AND so.mc_site_id = 1 AND ((dpt.payment_category = 1 AND so.order_payment_confirm_date >= trunc(to_date('"
				+ date
				+ "', 'yyyyMMdd')) AND so.order_payment_confirm_date < trunc(to_date('"
				+ date
				+ "', 'yyyyMMdd') + 1)) OR (dpt.payment_category = 2 AND so.order_create_time >= trunc(to_date('"
				+ date
				+ "', 'yyyyMMdd')) AND so.order_create_time < trunc(to_date('"
				+ date
				+ "', 'yyyyMMdd') + 1))) GROUP BY to_char(CASE WHEN dpt.payment_category = 1 THEN so.order_payment_confirm_date ELSE so.order_create_time END, 'hh24') ORDER BY to_char(CASE WHEN dpt.payment_category = 1 THEN so.order_payment_confirm_date ELSE so.order_create_time END, 'hh24')) a";


	}
	
	/**
	 * 批量插入 否则会报内存溢出 怀疑是多次new HTable导致
	 */
	public void insertRecordCustomer(String tableName, String columnFamily,
			List<RecoveryDataVo> list) throws IOException {
		int n = 1;
		HTable table = new HTable(configuration, tableName);
		List<Put> putList = new ArrayList<Put>();
		for (RecoveryDataVo rdv : list) {
			n++;
			String[] valueArr = new String[] { rdv.getColumn1(),rdv.getXTitle(), rdv.getXValue() };
			String[] qualifierArr = new String[] { "customer_num", "xTitle","xValue" };
			Put put = new Put(rdv.getRowkey().getBytes());
			for (int i = 0; i < qualifierArr.length; i++) {
				String col = qualifierArr[i];
				String val = valueArr[i];
				put.add(columnFamily.getBytes(), col.getBytes(), val.getBytes());
			}
			putList.add(put);
		}
		table.put(putList);
		table.flushCommits();
	}

	public List<RecoveryDataVo> getRecoveryDataCustomer(String today) {
		OracleConnection oc = new OracleConnection();
		List<RecoveryDataVo> list = new LinkedList<RecoveryDataVo>();
		RecoveryDataVo rdv = null;
		String sql = "SELECT to_char(CASE WHEN dpt.payment_category = 1 THEN so.order_payment_confirm_date ELSE so.order_create_time END, 'yyyymmdd hh24mi') date_id, count(distinct si.end_user_id) custm_num FROM stage_user.so_real so INNER JOIN stage_user.so_item_real SI ON so.id = si.order_id INNER JOIN dw.dim_payment_type dpt ON nvl(so.pay_service_type, 1) = dpt.id WHERE si.is_item_leaf = 1 AND so.mc_site_id = 1 AND ((dpt.payment_category = 1 AND so.order_payment_confirm_date >= trunc(to_date('"
				+ today
				+ "', 'yyyyMMdd')) AND so.order_payment_confirm_date < trunc(to_date('"
				+ today
				+ "', 'yyyyMMdd') + 1)) OR (dpt.payment_category = 2 AND so.order_create_time >= trunc(to_date('"
				+ today
				+ "', 'yyyyMMdd')) AND so.order_create_time < trunc(to_date('"
				+ today
				+ "', 'yyyyMMdd') + 1))) GROUP BY to_char(CASE WHEN dpt.payment_category = 1 THEN so.order_payment_confirm_date ELSE so.order_create_time END, 'yyyymmdd hh24mi') ORDER BY to_char(CASE WHEN dpt.payment_category = 1 THEN so.order_payment_confirm_date ELSE so.order_create_time END,'yyyymmdd hh24mi')";
		try {
			// 查询当天信息
			con = oc.getConnection();
			st = con.prepareStatement(sql);
			System.out.println("修复分钟顾客数==" + sql);
			rs = st.executeQuery();
			while (rs.next()) {
				String[] strLastXValue = null;
				String date_id = rs.getString(1);// ep 20140620 0001
				String customer_num = rs.getString(2);// ep 25286.84
				String[] rowkeyArr = date_id.split(" ");
				String date = rowkeyArr[0];
				String time = rowkeyArr[1];
				String keyTag = date + time;
				String rowkey = keyTag;
				SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
				Date tmpd = sdf.parse(keyTag);
				strLastXValue = CommonUtil.getCurFullXValueByOrdrTime(tmpd);
				String xTitle = strLastXValue[0];
				String xValue = strLastXValue[1];

				rdv = new RecoveryDataVo();
				rdv.setRowkey(rowkey);
				rdv.setColumn1(customer_num);
				rdv.setXTitle(xTitle);
				rdv.setXValue(xValue);
				list.add(rdv);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		} finally {
			oc.closeConnection(rs, st, con);
		}
		return list;
	}
	

	public boolean startRecoverDataCustomers() {
		List<Result> listHis;
		try {
			listHis = super.getRecordByRowKeyRegex("bi_monitor_customer_rslt", date);
			for (Result r : listHis) {
				super.deleteRecord("bi_monitor_customer_rslt", new String(r.getRow()));
			}
				List<RecoveryDataVo> list = this.getRecoveryDataCustomer(date);
				insertRecordCustomer("bi_monitor_customer_rslt",Constant.COMMON_FAMILY, list);
			System.out.println("分钟顾客数据修复完毕");
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(e.getClass());
		}
		return  true;
	}
	
	
	// 订单净额
	
	/**
	 * 恢复全站订单净额
	 */
	public boolean startRecoverDataOrderInfo() {
		tableName = Constant.TABLE_ORDER_INFO_RESULT;
		/** 线图 */
		rowKeyRegexLine = date + "_amount_";
		recoverLineData(tableName, rowKeyRegexLine);
		System.out.println("全站订单净额线图数据修复完成！");
		/** 柱状图 */
		rowKeyRegexCol = "col_data_" + date;
		recoverColData(tableName, rowKeyRegexCol);
		System.out.println("全站订单净额柱状图数据修复完成！");
		return true;
	}

	/**
	 * 恢复全站订单净额
	 * 
	 * @param tableName
	 * @param rowKeyRegexLine
	 */
	private void recoverLineData(String tableName, String rowKeyRegexLine) {
		// 首先需要干掉错误数据
		List<Result> listHis;
		try {
			listHis = super.getRecordByRowKeyRegex(tableName, rowKeyRegexLine);

			for (Result r : listHis) {
				super.deleteRecord(tableName, new String(r.getRow()));
			}
			// 查询oracle数据并写入hbase
			List<RecoveryDataVo> list = this.getRecoveryLineData(rowKeyRegexLine);
			insertRecordOrderInfo(tableName, Constant.COMMON_FAMILY, list);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 批量插入 否则会报内存溢出 怀疑是多次new HTable导致
	 * 
	 * @param tableName
	 * @param columnFamily
	 * @param list
	 * @throws IOException
	 */
	public void insertRecordOrderInfo(String tableName, String columnFamily,
			List<RecoveryDataVo> list) throws IOException {
		int n = 1;
		HTable table = new HTable(configuration, tableName);
		List<Put> putList = new ArrayList<Put>();
		for (RecoveryDataVo rdv : list) {
			n++;
			String[] valueArr = new String[] { rdv.getColumn1(),rdv.getXTitle(), rdv.getXValue() };
			String[] qualifierArr = new String[] { "order_amount", "xTitle","xValue" };
			Put put = new Put(rdv.getRowkey().getBytes());
			for (int i = 0; i < qualifierArr.length; i++) {
				String col = qualifierArr[i];
				String val = valueArr[i];
				put.add(columnFamily.getBytes(), col.getBytes(), val.getBytes());
			}
			putList.add(put);
		}
		table.put(putList);
		table.flushCommits();
		daoMsg = "插入行成功";
	}

	/**
	 * 恢复全站订单净额柱状图
	 * 
	 * @param tableName
	 * @param rowKeyRegexLine
	 */
	private void recoverColData(String tableName, String rowKeyRegexLine) {
		// 首先需要干掉错误数据
		List<Result> listHis;
		try {
			listHis = super.getRecordByRowKeyRegex(tableName, rowKeyRegexLine);

			for (Result r : listHis) {
				super.deleteRecord(tableName, new String(r.getRow()));
			}
			// 查询oracle数据并写入hbase
			List<RecoveryDataVo> list = this.getRecoveryColData();
			String colValue = "[";
			// 拼接小时字符串
			for (RecoveryDataVo rdv : list) {
				colValue += "[" + rdv.getColumn1() + "," + rdv.getColumn2()
						+ "],";
			}
			colValue = colValue.substring(0, colValue.length() - 1);
			colValue += "]";
			System.out.println("colValue==" + colValue);
			super.insertRecord(tableName, rowKeyRegexLine,Constant.COMMON_FAMILY, "col_data", colValue);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * 查询全站订单净额和移动订单净额 线图数据
	 * 
	 * @param rowKeyRegex
	 * @return
	 */
	public List<RecoveryDataVo> getRecoveryLineData(String rowKeyRegex) {
		OracleConnection oc = new OracleConnection();
		List<RecoveryDataVo> list = new LinkedList<RecoveryDataVo>();
		RecoveryDataVo rdv = null;
		try {
			// 查询当天信息
			con = oc.getConnection();
			st = con.prepareStatement(SQL_RECOVER_DATA_LINE, ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_UPDATABLE);
			System.out.println("修复线图==" + SQL_RECOVER_DATA_LINE);
			rs = st.executeQuery();
			while (rs.next()) {
				String[] strLastXValue = null;
				String date_id = rs.getString(1);// ep 20140620 0001
				String ordr_amt = rs.getString(2);// ep 25286.84
				String[] rowkeyArr = date_id.split(" ");
				String date = rowkeyArr[0];
				String time = rowkeyArr[1];
				String keyTag = date + time;
				String rowkey = rowKeyRegex + "1" + keyTag;
				SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
				Date tmpd = sdf.parse(keyTag);
				strLastXValue = CommonUtil.getCurFullXValueByOrdrTime(tmpd);
				String xTitle = strLastXValue[0];
				String xValue = strLastXValue[1];

				rdv = new RecoveryDataVo();
				rdv.setRowkey(rowkey);
				rdv.setColumn1(ordr_amt);
				rdv.setXTitle(xTitle);
				rdv.setXValue(xValue);
				list.add(rdv);
			}
			
			if (rs.last()) {
				String[] strLastXValue = null;
				String date_id = rs.getString(1);// ep 20140620 0001
				String ordr_amt = rs.getString(2);// ep 25286.84
				String[] rowkeyArr = date_id.split(" ");
				String date = rowkeyArr[0];
				String time = rowkeyArr[1];
				String keyTag = date + time;
				String rowkey = "last_amount_" + date;
				SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
				Date tmpd = sdf.parse(keyTag);
				strLastXValue = CommonUtil.getCurFullXValueByOrdrTime(tmpd);
				String xTitle = strLastXValue[0];
				String xValue = strLastXValue[1];

				rdv = new RecoveryDataVo();
				rdv.setRowkey(rowkey);
				rdv.setColumn1(ordr_amt);
				rdv.setXTitle(xTitle);
				rdv.setXValue(xValue);
				list.add(rdv);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		} finally {
			oc.closeConnection(rs, st, con);
		}
		return list;
	}

	/**
	 * 查询全站订单净额和移动订单净额 线图数据
	 * 
	 * @param rowKeyRegex
	 * @return
	 */
	public List<RecoveryDataVo> getRecoveryColData() {
		OracleConnection oc = new OracleConnection();
		List<RecoveryDataVo> list = new LinkedList<RecoveryDataVo>();
		RecoveryDataVo rdv = null;
		try {
			// 查询当天信息
			con = oc.getConnection();
			st = con.prepareStatement(SQL_RECOVER_DATA_COL);
			System.out.println("修复柱状图==" + SQL_RECOVER_DATA_COL);
			rs = st.executeQuery();
			while (rs.next()) {
				rdv = new RecoveryDataVo();
				rdv.setColumn1(rs.getString(1));
				rdv.setColumn2(rs.getString(2));
				list.add(rdv);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			oc.closeConnection(rs, st, con);
		}
		return list;
	}

	//移动顾客数
	/**
	 * 恢复移动顾客数
	 * @param date
	 * 操作日期 20140620
	 */
	public void startRecoverDataMobileCus() {
		List<Result> listHis;
		try {
			listHis = super.getRecordByRowKeyRegex("bi_monitor_mobile_customer_rslt", date);

			for (Result r : listHis) {
				super.deleteRecord("bi_monitor_mobile_customer_rslt", new String(r.getRow()));
			}
				List<RecoveryDataVo> list = this.getRecoveryDataMobileCus(date);
				insertRecordMobileCus("bi_monitor_mobile_customer_rslt",Constant.COMMON_FAMILY, list);
			System.out.println("分钟移动顾客数据修复完毕");
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(e.getClass());
		}
	}
	
	/**
	 * 批量插入 否则会报内存溢出 怀疑是多次new HTable导致
	 * 
	 * @param tableName
	 * @param columnFamily
	 * @param list
	 * @throws IOException
	 */
	public void insertRecordMobileCus(String tableName, String columnFamily,
			List<RecoveryDataVo> list) throws IOException {
		int n = 1;
		HTable table = new HTable(configuration, tableName);
		List<Put> putList = new ArrayList<Put>();
		for (RecoveryDataVo rdv : list) {
			n++;
			String[] valueArr = new String[] { rdv.getColumn1(),rdv.getXTitle(), rdv.getXValue() };
			String[] qualifierArr = new String[] { "customer_num", "xTitle","xValue" };
			Put put = new Put(rdv.getRowkey().getBytes());
			for (int i = 0; i < qualifierArr.length; i++) {
				String col = qualifierArr[i];
				String val = valueArr[i];
				put.add(columnFamily.getBytes(), col.getBytes(), val.getBytes());
			}
			putList.add(put);
		}
		table.put(putList);
		table.flushCommits();
	}


	public List<RecoveryDataVo> getRecoveryDataMobileCus(String today) {
		OracleConnection oc = new OracleConnection();
		List<RecoveryDataVo> list = new LinkedList<RecoveryDataVo>();
		RecoveryDataVo rdv = null;
		String sql = "SELECT to_char(CASE WHEN dpt.payment_category = 1 THEN so.order_payment_confirm_date ELSE so.order_create_time END, 'yyyymmdd hh24mi') date_id, count(distinct si.end_user_id) custm_num FROM stage_user.so_real so INNER JOIN stage_user.so_item_real SI ON so.id = si.order_id INNER JOIN dw.dim_payment_type dpt ON nvl(so.pay_service_type, 1) = dpt.id WHERE si.is_item_leaf = 1 AND so.mc_site_id = 1  AND so.order_source = 3 AND ((dpt.payment_category = 1 AND so.order_payment_confirm_date >= trunc(to_date('"
				+ today
				+ "', 'yyyyMMdd')) AND so.order_payment_confirm_date < trunc(to_date('"
				+ today
				+ "', 'yyyyMMdd') + 1)) OR (dpt.payment_category = 2 AND so.order_create_time >= trunc(to_date('"
				+ today
				+ "', 'yyyyMMdd')) AND so.order_create_time < trunc(to_date('"
				+ today
				+ "', 'yyyyMMdd') + 1))) GROUP BY to_char(CASE WHEN dpt.payment_category = 1 THEN so.order_payment_confirm_date ELSE so.order_create_time END, 'yyyymmdd hh24mi') ORDER BY to_char(CASE WHEN dpt.payment_category = 1 THEN so.order_payment_confirm_date ELSE so.order_create_time END,'yyyymmdd hh24mi')";
		try {
			// 查询当天信息
			con = oc.getConnection();
			st = con.prepareStatement(sql);
			System.out.println("修复移动分钟顾客数==" + sql);
			rs = st.executeQuery();
			while (rs.next()) {
				String[] strLastXValue = null;
				String date_id = rs.getString(1);// ep 20140620 0001
				String customer_num = rs.getString(2);// ep 25286.84
				String[] rowkeyArr = date_id.split(" ");
				String date = rowkeyArr[0];
				String time = rowkeyArr[1];
				String keyTag = date + time;
				String rowkey = keyTag;
				SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
				Date tmpd = sdf.parse(keyTag);
				strLastXValue = CommonUtil.getCurFullXValueByOrdrTime(tmpd);
				String xTitle = strLastXValue[0];
				String xValue = strLastXValue[1];

				rdv = new RecoveryDataVo();
				rdv.setRowkey(rowkey);
				rdv.setColumn1(customer_num);
				rdv.setXTitle(xTitle);
				rdv.setXValue(xValue);
				list.add(rdv);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		} finally {
			oc.closeConnection(rs, st, con);
		}
		return list;
	}
	
	/**
	 * 后台会传入四个参数
	 * @param args
	 * 第一个命令参数 recovery；第二"BI_CustomerAndSales"； 第三个 修复的应用； 第四个日期（20150601）
	 * 第3个参数说明：all_customer恢复全站顾客数,all_sale_amt恢复全站订单净额,all_mobile_customer恢复移动顾客数 all:恢复所有的顾客数与订单净额
	 * 例如修复 顾客分钟数storm jar storm-0.0.1-SNAPSHOT-jar-with-dependencies.jar com.yihaodian.bi.storm.common.topo.Main recovery BI_CustomersAndSales all_sale 20150601
	 */
	public static void  recovery(String[] args) {
		
		if (args == null || args.length < 4 || StringUtils.isEmpty(args[0])) {
			System.out.println("Missing params date or recovery or gpName or recoveryMethod.");
        }
    		final String biz=args[2];//修复的应用参数
    		final String date=args[3]; //日期参数
       try {
       if ("all_customer".equals(biz)) {
        	new CustomersAndSales(date).startRecoverDataCustomers();
        }else if("all_sale_amt".equals(biz)) {
        	new CustomersAndSales(date).startRecoverDataOrderInfo();
        }else if("all_mobile_customer".equals(biz)) {
        	new CustomersAndSales(date).startRecoverDataMobileCus();
        }else if ("all".equals(biz)) {
        	new CustomersAndSales(date).startRecoverDataCustomers();
        	new CustomersAndSales(date).startRecoverDataOrderInfo();
        	new CustomersAndSales(date).startRecoverDataMobileCus();
        }else {
        	System.out.println("修复的应用参数 is error.");
        }} catch (Exception e) {
			e.printStackTrace();
		}
    }

}
