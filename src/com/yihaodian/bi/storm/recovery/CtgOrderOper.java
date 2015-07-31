package com.yihaodian.bi.storm.recovery;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.yihaodian.bi.database.DBConnection;
import com.yihaodian.bi.database.impl.OracleConnection;
import com.yihaodian.bi.hbase.dao.GosOrderDao;
import com.yihaodian.bi.hbase.dao.impl.GosOrderDaoImpl;

public class CtgOrderOper {

	GosOrderDao dao;
	DBConnection dbcon;
	Connection con;
	public CtgOrderOper()
	{
		try {
			dao = new GosOrderDaoImpl();
			dbcon = new OracleConnection();
			con = dbcon.getConnection() ;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	//today  yyyy-MM-dd
	public boolean ctgOrderOper(String today)
	{
		if (today == null || "".equals(today)) {
			return false ;
		}
		StringBuffer sbBuffer = new StringBuffer() ;
		sbBuffer.append("  SELECT hc.categ_lvl1_name ctgName,hc.categ_lvl1_id, ");
		sbBuffer.append(" count(DISTINCT CASE WHEN so.parent_so_id >0 THEN  so.parent_so_id ELSE so.id END ) order_num, ");
		sbBuffer.append(" round(SUM(si.Order_Item_Amount),0) order_amt, ");
		sbBuffer.append(" SUM(si.Promotion_Amount) + sum(si.Coupon_Amount ) zhekou_amt, ");
		sbBuffer.append(" sum(si.Order_Item_Amount - NVL(si.Promotion_Amount,0) - NVL(si.Coupon_Amount,0) ) order_amt_jing, ");
		sbBuffer.append(" count(distinct si.end_user_id) customer_num, ");
		sbBuffer.append(" count(distinct si.product_id) sku_num ");
		sbBuffer.append(" FROM so_data2.so@so_link so ");
		sbBuffer.append(" INNER JOIN so_data2.so_item@so_link SI ON so.id = si.order_id  ");
		sbBuffer.append(" INNER JOIN dw.dim_prod dp on si.product_id=dp.prod_id AND dp.cur_flag = 1 ");
		sbBuffer.append(" INNER JOIN dw.hier_categ hc ON dp.categ_lvl_hier_skid = hc.categ_lvl_hier_skid AND hc.cur_flag = 1 ");
		sbBuffer.append(" INNER JOIN dw.dim_payment_type dpt ON  nvl(so.pay_service_type, 1)  = dpt.id ");
		sbBuffer.append(" join dw.dim_categ_lvl1 dcl on hc.categ_lvl1_id = dcl.categ_lvl1_id  ");
		sbBuffer.append("  WHERE   si.is_item_leaf = 1 and dcl.prod_dept_id in (1,2,3,5) ");
		sbBuffer.append(" AND  so.mc_site_id = 1 ");
		sbBuffer.append(" AND ( (dpt.payment_category = 1 AND so.order_payment_confirm_date >= date '"+today+"' and so.order_payment_confirm_date<date '"+today+"'+1) ");
		sbBuffer.append("  OR (dpt.payment_category = 2 AND so.order_create_time >= date '"+today+"' and so.order_create_time<date '"+today+"'+1)) ");
		sbBuffer.append(" GROUP BY hc.categ_lvl1_name,hc.categ_lvl1_id ");
		
		Statement statement = null;
		ResultSet rs = null;
		String orderDay = today.replaceAll("-", "") ;
		try {
			statement = con.createStatement();
			System.out.println(sbBuffer.toString());
			rs = statement.executeQuery(sbBuffer.toString()) ;
			while(rs.next())
			{
				String todayCtgId = orderDay+"_"+rs.getString("categ_lvl1_id") ;
				String order_num = rs.getString("order_num");
				String order_amt = rs.getString("order_amt");
				String customer_num = rs.getString("customer_num");
				String sku_num = rs.getString("sku_num") ;
				try {
					dao.deleteRecord("bi_monitor_ctg_rslt", todayCtgId);
					
					dao.insertGosOrder("bi_monitor_ctg_rslt", todayCtgId, "cf", "customer_num", customer_num+"");
					dao.insertGosOrder("bi_monitor_ctg_rslt", todayCtgId, "cf", "order_amount", order_amt+"");
					dao.insertGosOrder("bi_monitor_ctg_rslt", todayCtgId, "cf", "order_num", order_num+"");
					dao.insertGosOrder("bi_monitor_ctg_rslt", todayCtgId, "cf", "sku_num", sku_num+"");
					
//					dao.insertGosOrder("bi_monitor_ctg_rslt", todayCtgId, "cf", new String[]{"order_num","order_amount","customer_num","sku_num"}, new String[]{order_num+"",order_amt+"",customer_num+"",sku_num+""});
					System.out.println("done!==todayCtgId="+todayCtgId+";order_num="+order_num+";order_amt="+order_amt+";customer_num="+customer_num+";sku="+sku_num);
				} catch (Exception e) {
					System.out.println("write hbase error=="+e.getClass());
					e.printStackTrace();
				}
				
				
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(e.getClass());
			return false;
		}
		finally
		{
			try {
				rs.close();
				statement.close() ;
				con.close() ;
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return true;
	}
	
	
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println("2014-06-03".replaceAll("-", ""));
	}

}
