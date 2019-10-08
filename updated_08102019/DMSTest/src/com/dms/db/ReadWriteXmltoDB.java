package com.dms.db;


import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.dms.util.JDBCUtil;
import com.mysql.cj.result.Field;

public class ReadWriteXmltoDB {

	public Document databaseToXml(String dbTableNames[]) throws TransformerException,
			ParserConfigurationException, IOException {

			Connection con = null;
			PreparedStatement pstmt = null;
			ResultSet rs = null;
			DOMSource domSource = null;
		
			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
			DocumentBuilder builder = factory.newDocumentBuilder();
			Document doc = builder.newDocument();
			Element tables = doc.createElement("Tables");
			doc.appendChild(tables);
			con = JDBCUtil.getSourceDatabaseCon();
			
			try{			
				if(dbTableNames!=null && dbTableNames.length>0){
					for(String dbTableName : dbTableNames){
			pstmt = con
					.prepareStatement("select * from "+dbTableName);

			rs = pstmt.executeQuery();

			ResultSetMetaData rsmd = rs.getMetaData();//to retrieve table name, column name, column type and column precision, etc..
			int colCount = rsmd.getColumnCount();
			

		    DatabaseMetaData meta = con.getMetaData();
		     // The Oracle database stores its table names as Upper-Case,
		     // if you pass a table name in lowercase characters, it will not work.
		     // MySQL database does not care if table name is uppercase/lowercase.
		     //
		    ResultSet rs4Fk = meta.getImportedKeys(null, con.getCatalog(), dbTableName);
		    Map<String,String> refMap = new HashMap<String,String>(); 
		    while(rs4Fk.next()){
		       String fkColumnName = rs4Fk.getString("PKCOLUMN_NAME");
		       String fkTableName = rs4Fk.getString("PKTABLE_NAME");
		       refMap.put(fkColumnName, fkTableName);
		       
		     }
			
		    Element table = doc.createElement("Table");
		    tables.appendChild(table);
			
		    Element tableName = doc.createElement("TableName");
			tableName.appendChild(doc.createTextNode(rsmd.getTableName(1)));
			table.appendChild(tableName);

			Element structure = doc.createElement("TableStructure");
			table.appendChild(structure);

			Element col = null;
			for (int i = 1; i <= colCount; i++) {

				col = doc.createElement("Column" + i);
				table.appendChild(col);
				
				String curColName = rsmd.getColumnName(i);
				Element columnNode = doc.createElement("ColumnName");
				columnNode
						.appendChild(doc.createTextNode(rsmd.getColumnName(i)));
				col.appendChild(columnNode);

				Element typeNode = doc.createElement("ColumnType");
				typeNode.appendChild(doc.createTextNode(String.valueOf((rsmd
						.getColumnTypeName(i)))));
				col.appendChild(typeNode);

				Element lengthNode = doc.createElement("Length");
				lengthNode.appendChild(doc.createTextNode(String.valueOf((rsmd
						.getPrecision(i)))));
				col.appendChild(lengthNode);
				
				Element requiredNode = doc.createElement("Required");
				requiredNode.appendChild(doc.createTextNode(rsmd.isNullable(i)==0?String.valueOf(Boolean.TRUE):String.valueOf(Boolean.FALSE)));
				col.appendChild(requiredNode);
				
				Field field = ((com.mysql.cj.jdbc.result.ResultSetMetaData) rsmd).getFields()[i-1];
				boolean isPrimaryKey  = field.isPrimaryKey();
				if(isPrimaryKey){
					Element pkNode = doc.createElement("PrimaryKey");
					pkNode.appendChild(doc.createTextNode(String.valueOf((isPrimaryKey))));
					col.appendChild(pkNode);
				}
				
				
				boolean isForeignKey  = field.isMultipleKey();
				
				if(isForeignKey){
					Element fkNode = doc.createElement("RefKey");
					fkNode.appendChild(doc.createTextNode(String.valueOf((isForeignKey))));
					col.appendChild(fkNode);
					
					Element fkTable = doc.createElement("RefTable");
					fkTable.appendChild(doc.createTextNode(refMap.get(curColName)));
					col.appendChild(fkTable);
					
					
				}
				
				structure.appendChild(col);
			}

			//System.out.println("Column count = " + colCount);

			Element productList = doc.createElement("TableData");
			table.appendChild(productList);

			int l = 0;
			while (rs.next()) {
				Element row = doc.createElement(dbTableName + (++l));
				table.appendChild(row);
				for (int i = 1; i <= colCount; i++) {
					String columnName = rsmd.getColumnName(i);
					Object value = rs.getObject(i);
					Element node = doc.createElement(columnName);
					node.appendChild(doc.createTextNode((value != null) ? value
							.toString() : ""));
					row.appendChild(node);
				}
				productList.appendChild(row);
			}
			System.out.println("Created Structure of table :"+dbTableName);
					
		}
			
		}
		}catch (Exception e) {
			e.printStackTrace();
		}		
			doc.getDocumentElement().normalize();
			domSource = new DOMSource(doc);
			TransformerFactory tf = TransformerFactory.newInstance();
			Transformer transformer = tf.newTransformer();
			transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION,
					"yes");
			transformer.setOutputProperty(OutputKeys.METHOD, "xml");
			transformer.setOutputProperty(OutputKeys.ENCODING, "ISO-8859-1");

			File generateFile = new File("export.xml");
			if(!generateFile.exists()){
				generateFile.createNewFile();
			}
			StreamResult sr = new StreamResult(generateFile);
			transformer.transform(domSource, sr);
			
		

			System.out.println("Data has been generated in Current folder for the given table(s) with name export.xml");

			System.out.println("********************************");
			System.out.println("**************YOU CAN GIVE MULTIPLE TABLES EACH SEPARATED BY COMMA(,) Ex : Table1,Table2,Table3..  ******************");


		return doc;

	}

	public void xmlToDatabase() throws SQLException, ParserConfigurationException, SAXException, IOException

	{
		
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = factory.newDocumentBuilder();
		//Document doc = builder.parse(ReadWriteXmltoDB.class.getClass().getResourceAsStream("/tables.xml"));
		Document doc = builder.parse(new File("tables.xml"));
		 doc.getDocumentElement().normalize();

		Connection con = JDBCUtil.getDestDatbaseCon();

		NodeList tables = doc.getElementsByTagName("Table");
		
		List<String> createTablesDDl =  new ArrayList<String>();
		for (int temp = 0; temp < tables.getLength(); temp++) {

			Node nNode = tables.item(temp);
					
			if (nNode.getNodeType() == Node.ELEMENT_NODE) {
				
				StringBuffer createQuery = new StringBuffer();
				StringBuffer fkey= new StringBuffer();	
				
				Element eElement = (Element) nNode;
				
				boolean isTableExist = isTableExist(eElement.getAttribute("name"));
				/** DDL Start*/
				createQuery.append("create ");
				createQuery.append(nNode.getNodeName());
				
				createQuery.append(" ");
				/**Table Name*/
				createQuery.append(eElement.getAttribute("name"));
				createQuery.append("(");
				System.out.println("Table Name : " + eElement.getAttribute("name"));
				NodeList colList =  eElement.getElementsByTagName("column");
				
				for(int column = 0; column<colList.getLength();column++){
					Node colNode =  colList.item(column);
					NamedNodeMap colAttrs =  colNode.getAttributes();
					String colName = colAttrs.getNamedItem("name").getTextContent();
					String dataType = colAttrs.getNamedItem("datatype").getTextContent();
					String required = colAttrs.getNamedItem("required").getTextContent();
					String primaryKey = null;
					String refTab = null;
					String refCol = null;
					
					if(colAttrs.getNamedItem("ref_table")!=null){
						 refTab = colAttrs.getNamedItem("ref_table").getTextContent();
						refCol = colAttrs.getNamedItem("ref_col").getTextContent();
							
					}
					if(colAttrs.getNamedItem("primaryKey")!=null){
						primaryKey = colAttrs.getNamedItem("primaryKey").getTextContent();
							
					}
					createQuery.append(colName);
					createQuery.append(" ");
					createQuery.append(dataType);
					if(Boolean.valueOf(required)){
						createQuery.append(" not null, ");
					}else{
						createQuery.append(" , ");
					}
					if(Boolean.valueOf(primaryKey)){
						fkey.append("  PRIMARY KEY (");
						fkey.append(colName);
						fkey.append(")");
						
					}
					if(refTab!=null){
						fkey.append(" , CONSTRAINT FK_");
						fkey.append(colName);
						fkey.append(" FOREIGN KEY (");
						fkey.append(colName);
						fkey.append(")");
						fkey.append(" REFERENCES ");
						fkey.append(refTab);
						fkey.append("(");
						fkey.append(refCol);
						fkey.append(")");
					}
					
				}
				
				createQuery.append(fkey.toString());
				if(createQuery.toString().trim().endsWith(",")){
					String str = createQuery.toString().substring(0,createQuery.toString().length()-2);
					createQuery = new StringBuffer(str);
				}
				createQuery.append(")");
				
				createTablesDDl.add(createQuery.toString());
			}
		}
		
		NodeList alterTables = doc.getElementsByTagName("AlterTable");
		
		List<String> alterTablesDDl =  new ArrayList<String>();
		for (int temp = 0; temp < alterTables.getLength(); temp++) {

			Node nNode = alterTables.item(temp);
					
			if (nNode.getNodeType() == Node.ELEMENT_NODE) {
				
				
				Element eElement = (Element) nNode;
				NodeList colList =  eElement.getChildNodes();
				Node si = eElement.getNextSibling();
				
				for(int column = 0; column<colList.getLength();column++){
					
					StringBuffer createQuery = new StringBuffer();
					StringBuffer fkey= new StringBuffer();	
					
					/** DDL Start*/
					createQuery.append("alter table");
					
					createQuery.append(" ");
					/**Table Name*/
					createQuery.append(eElement.getAttribute("name"));
					
					Node colNode =  colList.item(column);
					
					NamedNodeMap colAttrs =  colNode.getAttributes();
					
					String colName = null;
					String dataType = null;
					String required = null;
					String refTab = null;
					String refCol = null;
					String colFromName = null;
					String colToName = null;
					String toType = null;
					
					if(colNode.getNodeName().equals("column_add")){
						System.out.println("1 add avai ");
						 colName = colAttrs.getNamedItem("name").getTextContent();
						dataType = colAttrs.getNamedItem("datatype").getTextContent();
						required = colAttrs.getNamedItem("required").getTextContent();
						
						if(colAttrs.getNamedItem("ref_table")!=null){
							 refTab = colAttrs.getNamedItem("ref_table").getTextContent();
							refCol = colAttrs.getNamedItem("ref_col").getTextContent();
								
						}
						createQuery.append(" add column ");
						createQuery.append(colName);
						createQuery.append(" ");
						createQuery.append(dataType);
						if(Boolean.valueOf(required)){
							createQuery.append(" not null, ");
						}else{
							createQuery.append(" , ");
						}
						
						if(refTab!=null){
							fkey.append(" alter table  ");
							fkey.append(eElement.getAttribute("name"));
							fkey.append(" add CONSTRAINT AL_FK_");
							fkey.append(System.currentTimeMillis());
							fkey.append(" FOREIGN KEY (");
							fkey.append(colName);
							fkey.append(")");
							fkey.append(" REFERENCES ");
							fkey.append(refTab);
							fkey.append("(");
							fkey.append(refCol);
							fkey.append(")");
						}
						//createQuery.append(fkey.toString());
						
					}else if(colNode.getNodeName().equals("column_del")){
						 colName = colAttrs.getNamedItem("name").getTextContent();
						 createQuery.append(" drop column ");
							createQuery.append(colName);
							createQuery.append(" ");
						
						
					}else if(colNode.getNodeName().equals("column_upd")){
						colFromName = colAttrs.getNamedItem("from_name").getTextContent();
						colToName = colAttrs.getNamedItem("to_name").getTextContent();
						toType = colAttrs.getNamedItem("to_type").getTextContent();
						
						createQuery.append(" change column ");
						createQuery.append(colFromName);
						createQuery.append(" ");
						createQuery.append(colToName);
						createQuery.append(" ");
						createQuery.append(toType);
						
					}else{
						continue;
					}
					
					if(createQuery.toString().trim().endsWith(",")){
						String str = createQuery.toString().substring(0,createQuery.toString().length()-2);
						createQuery = new StringBuffer(str);
					}
					alterTablesDDl.add(createQuery.toString());	
					if(fkey.length()>0){
						alterTablesDDl.add(fkey.toString());
					}
						
				}
				
				
			}
		}
		if(alterTablesDDl.size()>0){
			createTablesDDl.addAll(alterTablesDDl);
		}
		PreparedStatement prepStmt = null;
		for(String ddl : createTablesDDl){
			
			prepStmt = con.prepareStatement(ddl.toString());
			try{
				prepStmt.execute();
			}catch(SQLSyntaxErrorException e){
				System.out.println(" Exception while creating or altering the table "+e);
				System.out.println(" Continuing for other table creation/alter ");
				continue;
			}
			System.out.println(" tabled created/altered");
		}

	}

	private boolean isTableExist(String tableName) {
		
		PreparedStatement prepStmt = null;
			
			try{
				prepStmt = JDBCUtil.getDestDatbaseCon().prepareStatement("SELECT count(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ? LIMIT 1");
				prepStmt.setString(1, JDBCUtil.getDestDbSchema());
				prepStmt.setString(2, tableName);
				ResultSet rs = prepStmt.executeQuery();
				if(rs.next()){
					long noOfTables = rs.getLong(1);
					if(noOfTables == 1){
						return true;
					}else{
						return false;
					}
				}
			}catch(SQLSyntaxErrorException e){
				System.out.println(" Exception while creating or altering the table "+e);
				System.out.println(" Continuing for other table creation/alter ");
			}catch(SQLException e){
				System.out.println(tableName+" Tabel already exist");
				System.out.println(" Going for Al table creation/alter ");
			}
		
		
		return false;
	}

	
private boolean isColumnExist(String tableName,String columnName) {
		
		PreparedStatement prepStmt = null;
			
			try{
				prepStmt = JDBCUtil.getDestDatbaseCon().prepareStatement("SELECT count(*) FROM INFORMATION_SCHEMA .COLUMNS WHERE TABLE_SCHEMA =? AND TABLE_NAME =? and column_name = ?");
				prepStmt.setString(1, JDBCUtil.getDestDbSchema());
				prepStmt.setString(2, tableName);
				prepStmt.setString(3, columnName);
				ResultSet rs = prepStmt.executeQuery();
				if(rs.next()){
					long noOfColumns = rs.getLong(1);
					if(noOfColumns == 1){
						return true;
					}else{
						return false;
					}
				}
			}catch(SQLException e){
				System.out.println(" Errot while getting column info "+columnName);
			}
		
		
		return false;
	}
	
	;
	public String[] getAllTablesNames() {
		String sql = "SELECT table_name FROM information_schema.tables WHERE table_schema=? and table_type = ?";
				
		PreparedStatement prepStmt = null;
		List<String> tableNames = new ArrayList<String>();
		try{
			prepStmt = JDBCUtil.getDestDatbaseCon().prepareStatement(sql);
			prepStmt.setString(1, JDBCUtil.getDestDbSchema());
			prepStmt.setString(2, "BASE TABLE");
			ResultSet rs = prepStmt.executeQuery();
			while(rs.next()){
				tableNames.add(rs.getString(1));
			}
			
		}catch(SQLSyntaxErrorException e){
			System.out.println(" Exception while getting the table names "+e);
		}catch(SQLException e){
			System.out.println(" Exception while getting the table names"+e);
		}
	
	
	return tableNames.size()>0?tableNames.toArray(new String[tableNames.size()]):null;
	}

	
	public Set<String> generatedXmlToDatabase(){
		 try {
			File fXmlFile = new File("import.xml");
			if(!fXmlFile.exists()){
				System.out.println("****** There is no import.xml file found.. So, Assuming nothing to process.. If you want tables to be created/altered then please create the import.xml file and rerun the application ****");
				return null;
			}
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			Document doc = dBuilder.parse(fXmlFile);

			doc.getDocumentElement().normalize();

			Map<String,String> queries = new HashMap<String,String>();
			Set<String> logTables = new HashSet<String>();
			//System.out.println("Root element :" + doc.getDocumentElement().getNodeName());

			NodeList tableList = doc.getElementsByTagName("Table");

			System.out.println("----------------------------");

			for (int table = 0; table < tableList.getLength(); table++) {

				Node tableNode = tableList.item(table);
			if (tableNode.getNodeType() == Node.ELEMENT_NODE) {
				Element eElement = (Element) tableNode;
				StringBuffer createQuery = new StringBuffer();
				StringBuffer createLogQuery = new StringBuffer();
				StringBuffer fkey= new StringBuffer();	
				//System.out.println("\nCurrent Element :" + tableNode.getNodeName());
				
				Node tn = eElement.getElementsByTagName("TableName").item(0);
				String tableName = tn.getTextContent();
				System.out.println("*************** Processing the table "+tableName+" ***************");
				boolean isAlter = false;
				boolean isAlterTableRequired = false;
				boolean isPrimaryKeyAdded = false;
				if(!isTableExist(tableName)){
				
					/** DDL Start*/
					createQuery.append("create ");
					createQuery.append(tableNode.getNodeName());
					
					createQuery.append(" ");
					/**Table Name*/
					createQuery.append(tableName);
				
					createQuery.append("(");
				}else{
					System.out.println(tableName+" Table already exist!! So, going for alter");
					/** DDL Start*/
					createQuery.append("alter ");
					createQuery.append(tableNode.getNodeName());
					
					createQuery.append(" ");
					/**Table Name*/
					createQuery.append(tableName);
					isAlter = true;
					
				}
				
				

					Node struNode = eElement.getElementsByTagName("TableStructure").item(0);
					
				
					if (tableNode.getNodeType() == Node.ELEMENT_NODE) {
	
						Element strElement = (Element) struNode;
						NodeList colList = strElement.getChildNodes();
						for (int col = 0; col < colList.getLength(); col++) {
							Node colNode = colList.item(col);
							boolean primaryKey = false;
							String colName = null;
							String colType = null;
							String colLength = null;
							String required = null;
						
							String refTab = null;
							String refCol = null;
							if (colNode.getNodeType() == Node.ELEMENT_NODE) {
								Element colElement = (Element) colNode;
								 colName = colElement.getElementsByTagName("ColumnName").item(0).getTextContent();
								 if(isColumnExist(tableName, colName)){
									 System.out.println("Column "+colName+" already exist in the table "+tableName+" So, Checking for next column");
									 continue; 
								 }else{
									 System.out.println("Found New Column "+colName+" for the table "+tableName);
									 isAlterTableRequired = true;
								 }
								colType = colElement.getElementsByTagName("ColumnType").item(0).getTextContent();
								colLength = colElement.getElementsByTagName("Length").item(0).getTextContent();
								required = colElement.getElementsByTagName("Required").item(0).getTextContent();
							
								refTab = null;
								refCol = null;
								Node pk = colElement.getElementsByTagName("PrimaryKey").item(0);
								if(pk!=null){
									String isPrimaryKey = pk.getTextContent();
									System.out.println("Is Primary Key : "+isPrimaryKey);
									primaryKey = true;
								}
								
								Node fk = colElement.getElementsByTagName("RefKey").item(0);
								if(fk!=null){
									String isForeignKey = fk.getTextContent();
									System.out.println("Is Foreign Key : "+isForeignKey);
									String refTable = colElement.getElementsByTagName("RefTable").item(0).getTextContent();
									refCol = colName;
									refTab = refTable;
								}
								
								if(isAlter){
									createQuery.append(" add column ");
								}
								createQuery.append(colName);
								createQuery.append(" ");
								createQuery.append(colType );
								if(!colType.equalsIgnoreCase("DATETIME"))
								{
									createQuery.append("(");
									createQuery.append(colLength);
									createQuery.append(")");
								}
								if(Boolean.valueOf(required)){
									createQuery.append(" not null, ");
								}else{
								
									createQuery.append(" , ");
								}
															
								
								
								

							}
							
							if(Boolean.valueOf(primaryKey)){
								fkey.append("  PRIMARY KEY (");
								fkey.append(colName);
								fkey.append(")");
								isPrimaryKeyAdded = true;
								
							}
							
							if(refTab!=null){
								if(!isAlter){
									if(fkey.toString().trim().endsWith(",") || !isPrimaryKeyAdded){
										fkey.append("  CONSTRAINT FK_");
									}else{
										fkey.append(" , CONSTRAINT FK_");
									}
									
									fkey.append(colName);
									fkey.append("_");
									fkey.append(tableName);
									fkey.append(" FOREIGN KEY (");
								}else{
									fkey.append(" ADD FOREIGN KEY (");
								}
								
								fkey.append(colName);
								fkey.append(")");
								fkey.append(" REFERENCES ");
								fkey.append(refTab);
								fkey.append("(");
								fkey.append(refCol);
								fkey.append(")");
							}
							
							
							
						}
						
						createLogQuery.append(createQuery.toString().replace(tableName, tableName+"_Log"));
						
						createQuery.append(fkey.toString());
						if(createQuery.toString().trim().endsWith(",")){
							String str = createQuery.toString().substring(0,createQuery.toString().length()-2);
							createQuery = new StringBuffer(str);
						}
						if(!isAlter){
							createQuery.append(")");
						}
						System.out.println(createQuery);
						if(createLogQuery.toString().trim().endsWith(",")){
							String str = createLogQuery.toString().substring(0,createLogQuery.toString().length()-2);
							createLogQuery = new StringBuffer(str);
						}
						
						if(!isAlter){
							createLogQuery.append(", OPER VARCHAR(1) ,AUDIT_USER VARCHAR(100) , AUDIT_DATE DATETIME ");
							createLogQuery.append(")");
						}
						
						if(isAlterTableRequired){
							queries.put(tableName,createQuery.toString());
							
						}
						if(!isAlter && !tableName.toLowerCase().endsWith("_log")){
							queries.put(tableName+"_Log",createLogQuery.toString());
							//queries.put(tableName+"_BUD",createTrigger(tableName));
							logTables.add(tableName+"_Log");
						}
						System.out.println("***************"+tableName+ " Is processed *************");
					}

					//System.out.println(queries);
				NodeList tdList = eElement.getElementsByTagName("TableData");
				for (int tableData = 0; tableData < tdList.getLength(); tableData++) {
					Node colNode = tdList.item(tableData);
					
					// Process table data here
					
				}
				
			}
			
			}
			
			//System.out.println(queries);
			System.out.println("$$$$$$$$$$$$$$$$$$$ Proceeding with the database changes $$$$$$$$$$$$$$$$$$$");
			if(queries.entrySet().size()>0){
				
				
				if(executeQueries(queries,false)){
					System.out.println("Looks like there were few tables missing where foreign key refered.. checking again");
					executeQueries(queries,false);
					return logTables;
				}
			}else{
				System.out.println(" Database is up to date.. No need to create or alter");
				return null;
			}
	    } catch (Exception e) {
		e.printStackTrace();
	    }
		 return null;
	}

	

	public void createTrigger(Set<String> tableNames) {
		try {
			PreparedStatement pstmt = null;
			Connection con = JDBCUtil.getDestDatbaseCon();
			
			Map<String,String> triggers = new HashMap<String,String>();
			
			for(String table : tableNames){
			List<String> columns = new ArrayList<String>();
			pstmt = con
					.prepareStatement("select * from "+table);

			ResultSet rs = pstmt.executeQuery();

			ResultSetMetaData rsmd = rs.getMetaData();//to retrieve table name, column name, column type and column precision, etc..
			int colCount = rsmd.getColumnCount();
			
			for (int column = 1; column <= colCount; column++) {
				
				columns.add(rsmd.getColumnName(column));
				
			}
			triggers.put(table+"_UPD",createTrigger(table,columns,true));
			triggers.put(table+"_DEL",createTrigger(table,columns,false));
			}
			
			executeQueries(triggers,true);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//trigger.append(" CREATE OR REPLACE TRIGGER ");
		
		
	}

	private String createTrigger(String table, List<String> columns,boolean isUpdate) {
		StringBuffer trigger = new StringBuffer();
		String originalTable = table.split("_")[0];
		trigger.append(" CREATE TRIGGER ");
		trigger.append(originalTable);
		if(isUpdate){
			trigger.append("_BUD_UPD BEFORE update ON ");
		}else{
			trigger.append("_BUD_DEL BEFORE delete ON ");
		}
		trigger.append(originalTable);
		trigger.append(" FOR EACH ROW ");
		trigger.append(" insert into ");
		trigger.append(table);
		trigger.append("(");
		StringBuffer columnNames = new StringBuffer();
		for(String column : columns){
			columnNames.append(column);
			columnNames.append(",");
		}
		trigger.append(columnNames.substring(0,columnNames.length()-1));
		trigger.append(")");
		trigger.append(" values (");
		columnNames = new StringBuffer();
		for(int c = 0;c<columns.size()-3;c++){
			columnNames.append("old.");
			columnNames.append(columns.get(c));
			columnNames.append(",");
		}
		trigger.append(columnNames);
		if(isUpdate){
			trigger.append("'U'");	
		}else{
			trigger.append("'D'");
		}
		
		trigger.append(", system_user(), NOW()");
		trigger.append(")"); 
		
		return trigger.toString();
	}
		

	/**
	 * @param queries
	 * @param con
	 * @param isexception
	 * @return
	 * @throws SQLException
	 */
	private boolean executeQueries(Map<String, String> queries,boolean isTrigger)
			throws SQLException {
		boolean isexception = false;
		PreparedStatement prepStmt = null;
		Connection con = JDBCUtil.getDestDatbaseCon();
		for(Map.Entry query : queries.entrySet()){
				
				prepStmt = con.prepareStatement(query.getValue().toString());
				try{
					prepStmt.execute();
				}catch(Exception e){
					System.err.println(" Error while creating or altering : "+query.getKey()+", as "+e.getMessage());
					System.out.println(" Continuing for other table creation/alter ");
					isexception = true;
					continue ;
					
				}
				if(isTrigger){
					System.out.println(query.getKey() +" trigger created ");
				}else{
					System.out.println(query.getKey() +" table created/altered");
				}
				
			}
		return isexception;
	}

	public String[] getAllLogTables() {
		String sql = "SELECT table_name FROM information_schema.tables WHERE table_schema=? and table_type = ? and table_name like '%_Log'";
				
		PreparedStatement prepStmt = null;
		List<String> tableNames = new ArrayList<String>();
		try{
			prepStmt = JDBCUtil.getDestDatbaseCon().prepareStatement(sql);
			prepStmt.setString(1, JDBCUtil.getDestDbSchema());
			prepStmt.setString(2, "BASE TABLE");
			ResultSet rs = prepStmt.executeQuery();
			while(rs.next()){
				tableNames.add(rs.getString(1));
			}
			
		}catch(SQLSyntaxErrorException e){
			System.out.println(" Exception while getting the table names "+e);
		}catch(SQLException e){
			System.out.println(" Exception while getting the table names"+e);
		}
	
	
	return tableNames.size()>0?tableNames.toArray(new String[tableNames.size()]):null;
	}
	
	
	
	
	
	
}
