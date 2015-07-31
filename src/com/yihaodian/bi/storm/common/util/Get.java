package com.yihaodian.bi.storm.common.util;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yihaodian.bi.database.DBConnection;
import com.yihaodian.bi.database.impl.OracleConnection;
import com.yihaodian.bi.hbase.dao.BaseDao;
import com.yihaodian.bi.hbase.dao.GosOrderDao;

public class Get {

	private static final Logger logger = LoggerFactory.getLogger(Get.class);
	private static DBConnection dBConnection;
	private static Connection con;
	/**
	 * 根据product_id查询hbase表，得到categ_lvl1_id
	 * @param dao
	 * @param product_id
	 * @return
	 */
	public static String getCtgIDFromProd(GosOrderDao dao,String product_id)
	{
		String ctgId = null;
		if (product_id == null) {
			return ctgId ;
		}
		try {
			Result result = dao.getOneRecord("prod_ctgLv1", product_id);
			for (KeyValue keyValue : result.raw()) {
				if ("categ_lvl1_id".equals(new String(keyValue.getQualifier()))) {
					ctgId = new String(keyValue.getValue()) ;
					break;
				}
			}
		} catch (IOException e) {
			logger.error("getCtgIDFromProd", e);
		}
		return ctgId;
	}
	
	/**
	 * 根据merchant_id查询hbase表dim_mrchnt，得到biz_unit
	 * @param dao
	 * @param merchant_id
	 * @return
	 */
	public static String getBizUnitByMrchtId(GosOrderDao dao,String merchant_id)
	{
		String biz_unit = null;
		if (merchant_id == null) {
			return biz_unit ;
		}
		try {
			Result result = dao.getOneRecord("dim_mrchnt", merchant_id);
			for (KeyValue keyValue : result.raw()) {
				if ("biz_unit".equals(new String(keyValue.getQualifier()))) {
					biz_unit = new String(keyValue.getValue()) ;
					break;
				}
			}
		} catch (IOException e) {
			logger.error("getCtgIDFromProd", e);
		}
		return biz_unit;
	}
/**
 * 根据prodId查询hbase表prod_info 得到column_name
 * @param dao
 * @param prodId
 * @param column_name 要获取的字段categ_lvl1_id，categ_lvl2_id，mg_brand_id
 * @return
 */
	public static String getCtgIDorBrandIDFromProd(GosOrderDao dao,String prodId,String column_name)
	{ 
		String ctgIDorBrandID=null;
		if(prodId==null || column_name ==null) {
			return ctgIDorBrandID;
		}else { 
			try {
				ctgIDorBrandID=dao.getColumnValue("prod_info", prodId, "cf", column_name);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return ctgIDorBrandID;
	}
	
	public static Map<String,String> getPordType() throws SQLException {
		dBConnection = new OracleConnection();
		con = dBConnection.getConnection() ;
		Statement st = con.createStatement();
		Map<String,String> m = new HashMap<String,String>();
		String sql="SELECT prod_id,\r\n" + 
				"       prod_type\r\n" + 
				"FROM   dw.dim_prod p\r\n" + 
				"WHERE  p.cur_flag = 1\r\n" + 
				"AND    p.prod_type IN (4, 7)";
		ResultSet rs = st.executeQuery(sql);
		while (rs.next()) {
			m.put(rs.getString(1), rs.getString(2));
		}
		return m;
	}
	
	/**
	 * 
	 * @param dao
	 * @param tracker_u
	 * @return
	 * @throws IOException
	 */
	public static String getChannel(BaseDao dao, String tracker_u) throws IOException {
		return dao.getColumnValue(Constant.HBASE_DIM_CHANNEL, tracker_u, 
				Constant.COMMON_FAMILY, "lvl1_chanl_id");
	}

}
