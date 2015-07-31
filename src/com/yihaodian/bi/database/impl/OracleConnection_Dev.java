package com.yihaodian.bi.database.impl;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.yihaodian.bi.database.DBConnection;

public class OracleConnection_Dev implements DBConnection {

	private final String DRIVER = "oracle.jdbc.driver.OracleDriver";

	private final String URL = "jdbc:oracle:thin:@192.168.20.56:1521/BIDEV";

	private final String USERNAME = "edw1_user";

	private final String PASSWORD = "edw1_user";

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
