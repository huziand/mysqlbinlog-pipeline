package com.jd.binlog.server;

import com.mysql.jdbc.Driver;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class JProxyConnector {

	private static final Logger LOGGER = Logger.getLogger(JProxyConnector.class);

	private String connectionUrl;
	private Properties connectionProperties = new Properties();
	private Driver driver;

	public JProxyConnector(BinlogPipelineConfig config) {
		connectionProperties.setProperty("user" ,config.getJProxyUser());
		connectionProperties.setProperty("password" ,config.getJProxyPassword());
		try {
			driver = new Driver();
		} catch (SQLException e) {
			LOGGER.info(e.getMessage());
			e.printStackTrace();
		}
		this.connectionUrl = config.getJProxyAddress();
	}

	public Connection getConnection() throws SQLException {
		return driver.connect(connectionUrl, connectionProperties);
	}
	
	
	public static void close(ResultSet rs, Statement stmt, Connection conn) {
		try {
			if (rs != null)
				rs.close();
		} catch (SQLException e) {
			LOGGER.error("close ResultSet error !!" + e.getMessage());
		} 

		try {
			if (stmt != null)
				stmt.close();
		} catch (SQLException e) {
			LOGGER.error("close Statement error !!" + e.getMessage());
		} 

		try {
			if (conn != null)
				conn.close();
		} catch (SQLException e) {
			LOGGER.error("close Connection error !!" + e.getMessage());
		}
	}
}
