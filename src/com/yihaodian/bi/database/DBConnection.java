package com.yihaodian.bi.database;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public interface DBConnection {
	// 获取数据库连接
	public Connection getConnection();
	
	// 关闭数据库连接
	public void closeConnection(ResultSet set, PreparedStatement pre,
			Connection conn);
}