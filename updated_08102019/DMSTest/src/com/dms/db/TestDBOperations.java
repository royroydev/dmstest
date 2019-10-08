package com.dms.db;

import java.util.HashSet;
import java.util.Set;

import com.dms.util.JDBCUtil;

public class TestDBOperations {

	public static void main(String arg[]) {


	
		ReadWriteXmltoDB readWrite = new ReadWriteXmltoDB();
		try {
			System.out.println("**** Welcome to DMS Application ****");
			String tableNames = JDBCUtil.getProp().getProperty("table.names");
			if(tableNames!=null && tableNames.length()>0){
				String tables[] = tableNames.split(",");
				//Generate the xml with data from given table				
				readWrite.databaseToXml(tables);
			}else{
				System.out.println("You have not given any table names in param.properties file. So, proceeding with all the tables in the schema "+JDBCUtil.getSourceDbSchema());
				String tables [] = readWrite.getAllTablesNames();
				readWrite.databaseToXml(tables);
			}
			
			
			Set<String> tables = readWrite.generatedXmlToDatabase();
			if(tables!=null && tables.size()>0){
				readWrite.createTrigger(tables);
			}else{
				System.out.println("Triggers are available for all tables.");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}


	}

	
	}
