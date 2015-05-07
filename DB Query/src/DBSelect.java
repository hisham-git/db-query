

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import com.mysql.jdbc.exceptions.MySQLSyntaxErrorException;

public class DBSelect {

	// Connection url Example: "jdbc:mysql://ipaddr:port/db_name"

	//private static String dbUrl = "jdbc:mysql://dev-d0003.c6udo7rwtj1j.us-east-1.rds.amazonaws.com:3306/hir_db";
	//private static String username = "hir_user";
	//private static String password = "x775CArwkqpUG2vY";

	//private static String dbUrl = "jdbc:mysql://devinstance3.ckalxfhzivms.us-east-1.rds.amazonaws.com:3306/jxntm4_2_49";
	//private static String username = "jdbcusr";
	//private static String password = "password";

	//private static String dbUrl = "jdbc:mysql://10.0.0.112:3306/jxntm_gatewaygl";
	//private static String username = "root";
	//private static String password = "dynamic";


	private String host;
	private String dbName;
	private String dbType;
	private String driver;
	private String url;
	private String username;
	private String password;

	public Connection getDBConnection(String setHost, String setName, String setType) throws Exception {

		if( setType.equalsIgnoreCase("MySQL") ) {

			this.driver = "com.mysql.jdbc.Driver";
			this.dbType = setType.toLowerCase();

			if( (setHost.equals("dev-d0003")) ) {
				this.host = setHost;
				this.dbName = setName;
				
				this.url = "jdbc:" + dbType +"://" + host + ".c6udo7rwtj1j.us-east-1.rds.amazonaws.com:3306/" + dbName;
				this.username = "hir_user";
				this.password = "x775CArwkqpUG2vY";
				
			} else if( (setHost.equals("test-d0003")) ) {
				this.host = setHost;
				this.dbName = setName;
				
				this.url = "jdbc:" + dbType +"://" + host + ".c6udo7rwtj1j.us-east-1.rds.amazonaws.com:3306/" + dbName;
				this.username = "hir_user";
				this.password = "eChHPhX6KzWk7q9x";
				
			} else if( (setHost.equals("devinstance3")) ) {
				this.host = setHost;
				this.dbName = setName;

				this.url = "jdbc:" + dbType +"://" + host + ".ckalxfhzivms.us-east-1.rds.amazonaws.com:3306/" + dbName;
				this.username = "jdbcusr";
				this.password = "password";
				
			} else if( (setHost.equals("10.0.0.112")) ) {
				this.host = setHost;
				this.dbName = setName;

				this.url = "jdbc:" + dbType +"://" + host + ":3306/" + dbName;
				this.username = "root";
				this.password = "dynamic";
			
			} else if( (setHost.equals("10.0.0.2")) ) {
				this.host = setHost;
				this.dbName = setName;

				this.url = "jdbc:" + dbType +"://" + host + ":3306/" + dbName;
				this.username = "dsi";
				this.password = "dynamic312";	
				
			} else if( ((setHost.equals("localhost")) || (setHost.equals("10.0.0.121"))) ) {
				this.host = setHost;
				this.dbName = setName;

				this.url = "jdbc:" + dbType +"://" + host + ":3306/" + dbName;
				this.username = "root";
				this.password = "hisham123";
				
			} else {
				return null;	
			}
			
			return returnConnection();

		} else if( setType.equalsIgnoreCase("SQLServer") ) {

			this.driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
			this.dbType = setType.toLowerCase();

			if( (setHost.equals("10.0.0.112")) ) {
				this.host = setHost;
				this.dbName = setName;
				this.dbType = setType;

				this.url = "jdbc:" + dbType +"://" + host + ";databaseName=" + dbName;
				this.username = "sa";
				this.password = "WKfs130";
	
			} else {
				return null;	
			}
			
			return returnConnection();

		} else if( setType.equalsIgnoreCase("Oracle") ) {

			this.driver = "oracle.jdbc.driver.OracleDriver";
			this.dbType = setType.toLowerCase();

			if( (setHost.equals("10.0.0.2")) ) {
				this.host = setHost;
				this.dbName = setName;
				this.dbType = setType;

				this.url = "jdbc:" + dbType + ":thin:@" + host + ":1522:"+ dbName;
				this.username = "jdbcmgr";
				this.password = "password";

			} else {
				return null;	
			}
			
			return returnConnection();

		} else {
			return null;
		}

	}
	
	private Connection returnConnection() throws MySQLSyntaxErrorException, ClassNotFoundException, SQLException {
		Connection conn = null;
		try {
			Class.forName(driver); // Register Specific JDBC driver
			conn = DriverManager.getConnection(url, username, password);
			return conn;
		} catch (MySQLSyntaxErrorException e) {
			System.out.println("Database " + dbName + " not found");
		}
		return null;
		
	}



/*	public static Connection getMySqlConnection() throws Exception {
		String driver = "com.mysql.jdbc.Driver";
		String url = "jdbc:mysql://devinstance3.ckalxfhzivms.us-east-1.rds.amazonaws.com:3306/jxntm_payment_gateway";
		String username = "jdbcusr";
		String password = "password";
		Class.forName(driver); // Register MySQL JDBC driver
		Connection conn = DriverManager.getConnection(url, username, password);
		return conn;
	}


	public static Connection getSQLServerConnection() throws Exception {
		String driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
		String url = "jdbc:sqlserver://10.0.0.112;databaseName=jxntm42_zakir";
		String username = "sa";
		String password = "WKfs130";
		Class.forName(driver); // Register SQLServer JDBC driver
		Connection conn = DriverManager.getConnection(url, username, password);
		return conn;
	}


	public static Connection getOracleConnection() throws Exception {
		String driver = "oracle.jdbc.driver.OracleDriver";
		String url = "jdbc:oracle:thin:@10.0.0.2:1522:orcl";
		String username = "jdbcmgr";
		String password = "password";
		Class.forName(driver); // Register Oracle JDBC driver
		Connection conn = DriverManager.getConnection(url, username, password);
		return conn;
	}*/
}