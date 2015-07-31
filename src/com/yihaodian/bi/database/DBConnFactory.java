package com.yihaodian.bi.database;

import com.yihaodian.bi.database.impl.MysqlConnection;
import com.yihaodian.bi.database.impl.OracleConnection;

/**
 * 数据库工厂类,用户获取不同的数据库连接
 * 
 */
public class DBConnFactory {
	/**
	 * 
	 * @param dbType
	 *            数据库类型 mysql,oracle ...
	 * @return
	 * @throws Exception
	 */
	public static DBConnection getConnection(String dbType) {
		DBConnection conn = null;
		if ("mysql".equals(dbType)) {
			conn = new MysqlConnection();
		} else if ("oracle".equals(dbType)) {
			conn = new OracleConnection();
		} else {
			try {
				throw new Exception("无法获取数据源!");
			} catch (Exception e) {
				e.getLocalizedMessage();
			}
		}
		return conn;
	}

}
