package com.dms.util;

import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class JDBCUtil {
	static Connection sourceDatabaseCon = null;
	static Connection destDatbaseCon = null;
	static Properties prop = new Properties();
	
	static{
	
		 try (InputStream input = new FileInputStream("param.properties")) {
	            
	            prop.load(input);
	            setSourceDatabaseConnection(getSourceDatabaseType());
	            setDestDatabaseConnection(getDestDatabaseType());
				
		 }catch (Exception e) {
			 e.printStackTrace();
		}
	}

	
	
	public static Properties getProp() {
		return prop;
	}


	private static void setSourceDatabaseConnection(String sourceDatabase) {

		try {
			if("mysql".equalsIgnoreCase(sourceDatabase)){
				Class.forName(prop.getProperty("mysql.db.driver.class"));
				sourceDatabaseCon = DriverManager.getConnection(prop.getProperty("mysql.db.url")+getSourceDbSchema(),prop.getProperty("mysql.db.user"),prop.getProperty("mysql.db.password"));
			}else if("oracle".equalsIgnoreCase(sourceDatabase)){
				Class.forName(prop.getProperty("oracle.db.driver.class"));
				sourceDatabaseCon = DriverManager.getConnection(prop.getProperty("oracle.db.url"),prop.getProperty("oracle.db.user"),prop.getProperty("oracle.db.password"));
			}else if("sql".equalsIgnoreCase(sourceDatabase)){
				Class.forName(prop.getProperty("sql.db.driver.class"));
				sourceDatabaseCon = DriverManager.getConnection(prop.getProperty("sql.db.url"),prop.getProperty("sql.db.user"),prop.getProperty("sql.db.password"));
			}else if("postgress".equalsIgnoreCase(sourceDatabase)){
				Class.forName(prop.getProperty("postgress.db.driver.class"));
				sourceDatabaseCon = DriverManager.getConnection(prop.getProperty("postgress.db.url"),prop.getProperty("postgress.db.user"),prop.getProperty("postgress.db.password"));
			}  
		}catch (Exception e) {
				System.out.println("Exception while getting the dest database connection");
		}
		
	}
	private static void setDestDatabaseConnection(String destDatabase) {
		try {
			if("mysql".equalsIgnoreCase(destDatabase)){
				Class.forName(prop.getProperty("mysql.db.driver.class"));
				destDatbaseCon = DriverManager.getConnection(prop.getProperty("mysql.db.url")+getDestDbSchema(),prop.getProperty("mysql.db.user"),prop.getProperty("mysql.db.password"));
			}else if("oracle".equalsIgnoreCase(destDatabase)){
				Class.forName(prop.getProperty("oracle.db.driver.class"));
				destDatbaseCon = DriverManager.getConnection(prop.getProperty("oracle.db.url"),prop.getProperty("oracle.db.user"),prop.getProperty("oracle.db.password"));
			}else if("sql".equalsIgnoreCase(destDatabase)){
				Class.forName(prop.getProperty("sql.db.driver.class"));
				destDatbaseCon = DriverManager.getConnection(prop.getProperty("sql.db.url"),prop.getProperty("sql.db.user"),prop.getProperty("sql.db.password"));
			}else if("postgress".equalsIgnoreCase(destDatabase)){
				Class.forName(prop.getProperty("postgress.db.driver.class"));
				destDatbaseCon = DriverManager.getConnection(prop.getProperty("postgress.db.url"),prop.getProperty("postgress.db.user"),prop.getProperty("postgress.db.password"));
			}  
		}catch (Exception e) {
				System.out.println("Exception while getting the dest database connection");
		}
				
	}
		



	public static Connection getSourceDatabaseCon() {
		return sourceDatabaseCon;
	}


	public static Connection getDestDatbaseCon() {
		return destDatbaseCon;
	}


	public static String getSourceDatabaseType() {
		return prop.getProperty("source.database.type");
	}


	public static String getDestDatabaseType() {
		return prop.getProperty("dest.database.type");
	}

	
	
	
	public static String getSourceDbSchema() {
		return prop.getProperty("source.db.schema");
	}


	

	public static String getDestDbSchema() {
		return prop.getProperty("dest.db.schema");
	}


	


	public static void cleanup(Connection con ,Statement ps,ResultSet rs){
		try {
			if(null != con)
				con.close();
			if(null != ps)
				ps.close();
			if(null != rs)
				rs.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}


	
	

}
