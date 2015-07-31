package com.yihaodian.bi.storm.recovery;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.yihaodian.bi.database.DBConnection;
import com.yihaodian.bi.database.impl.OracleConnection;
import com.yihaodian.bi.hbase.dao.GosOrderDao;
import com.yihaodian.bi.hbase.dao.impl.GosOrderDaoImpl;

public class AreaOrderOper {

	GosOrderDao dao;
	DBConnection dbcon;
	Connection con;
	public AreaOrderOper()
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
	public boolean areaOrderOper(String today)
	{
		if (today == null || "".equals(today)) {
			return false ;
		}
		StringBuffer sbBuffer = new StringBuffer() ;
		sbBuffer.append("SELECT nvl(a.REGN_NAME,'上海') regn_name,nvl(a.REGN_ID,1) regn_id, ");
		sbBuffer.append("count(DISTINCT CASE WHEN so.parent_so_id >0 THEN  so.parent_so_id ELSE so.id END ) order_num, ");
		sbBuffer.append("round(SUM(si.Order_Item_Amount),0) order_amt, ");
		sbBuffer.append(" SUM(si.Promotion_Amount) + sum(si.Coupon_Amount ) zhekou_amt, ");
		sbBuffer.append(" sum(si.Order_Item_Amount - NVL(si.Promotion_Amount,0) - NVL(si.Coupon_Amount,0) ) order_amt_jing, ");
		sbBuffer.append("count(distinct si.end_user_id) customer_num, ");
		sbBuffer.append("count(distinct si.product_id) sku_num  ");
		sbBuffer.append("FROM so_data2.so@so_link so ");
		sbBuffer.append("INNER JOIN so_data2.so_item@so_link SI ON so.id = si.order_id  ");
		sbBuffer.append("LEFT JOIN dw.dim_prov p ON so.good_receiver_province_id = p.prov_id AND p.cur_flag =1 ");
		sbBuffer.append("LEFT join dw.dim_regn a ON p.regn_id = a.REGN_ID AND a.cur_flag = 1 ");
		sbBuffer.append("INNER JOIN dw.dim_payment_type dpt ON  nvl(so.pay_service_type, 1)  = dpt.id  ");
		sbBuffer.append("WHERE   si.is_item_leaf = 1 AND    so.mc_site_id = 1 ");
		sbBuffer.append(" AND ( (dpt.payment_category = 1 AND so.order_payment_confirm_date >= date '"+today+"' and so.order_payment_confirm_date<date '"+today+"'+1) ");
		sbBuffer.append("       OR (dpt.payment_category = 2 AND so.order_create_time >= date '"+today+"' and so.order_create_time<date '"+today+"'+1))");
		sbBuffer.append("  GROUP BY nvl(a.REGN_NAME,'上海'),nvl(a.REGN_ID,1) ");
		
		
		Statement statement = null;
		ResultSet rs = null;
		String orderDay = today.replaceAll("-", "") ;
		try {
			statement = con.createStatement();
			System.out.println(sbBuffer.toString());
			rs = statement.executeQuery(sbBuffer.toString()) ;
			while(rs.next())
			{
				String todayId = orderDay+"_"+rs.getString("regn_id") ;
//				int order_num = rs.getInt("order_num");
				double order_amt = rs.getDouble("order_amt");
				int customer_num = rs.getInt("customer_num");
				int sku_num = rs.getInt("sku_num") ;
				
				try {
					dao.deleteRecord("bi_monitor_area_accumulative_rslt", todayId);
					
					dao.insertGosOrder("bi_monitor_area_accumulative_rslt", todayId, "cf", new String[]{"pro_amount","cust_num","pro_num"}, new String[]{order_amt+"",customer_num+"",sku_num+""});
					System.out.println("done!==todayId="+todayId+";order_amt="+order_amt+";sku_num="+sku_num+";customer_num="+customer_num);
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
