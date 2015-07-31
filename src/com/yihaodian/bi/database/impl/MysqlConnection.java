package com.yihaodian.bi.database.impl;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.yihaodian.bi.database.DBConnection;

public class MysqlConnection implements DBConnection {

	private final String DRIVER = "com.mysql.jdbc.Driver";

	private final String URL = "jdbc:mysql://localhost:3306/storm";

	private final String USERNAME = "root";

	private final String PASSWORD = "123";

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
}
