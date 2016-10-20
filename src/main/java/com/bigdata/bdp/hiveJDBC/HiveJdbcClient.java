package com.bigdata.bdp.hiveJDBC;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;

import com.bigdata.bdp.constants.Constants;
import com.bigdata.bdp.utils.BigDataUtils;

/**
 * This class HiveJdbcClient will run Hive queries using HiveServer 2 by using
 * JDBC driver.
 * 
 * 
 * @author Sai Kokadwar
 * @version 1.0
 * @since 2016-08-04
 * 
 */

public class HiveJdbcClient implements Constants{
	
	private static Connection conn = null;
	private final static Logger logger = Logger.getLogger(HiveJdbcClient.class);
	
	public static void main(String[] args) {
		String hiveServer2Jdbc = args[0];
		String userName = args[1];
		String password = args[2];
		String queryForQueue = args[3];
		String queryForExtract = args[4];
		String beelineExternalPath = args[5];
		String beelineDBName = args[6];
		
		HiveJdbcClient jdbcClient = new HiveJdbcClient();
		
		jdbcClient.createConnection(hiveServer2Jdbc,userName,password,beelineDBName);
		jdbcClient.executeQueryForQueue(BigDataUtils.readQueryFile(queryForQueue));
		jdbcClient.executeQueryForOverWrite(BigDataUtils.readQueryFile(queryForExtract),beelineExternalPath,beelineDBName);
		jdbcClient.closeConnection();

	}

	/**
	 * Closing JDBC Connection
	 * 
	 * @throws Exception
	 * 
	 */
	private void closeConnection() {
		if (conn != null)
			try {
				conn.close();
			} catch (SQLException e) {
				logger.info("SQL Exception occured while closing connection", e);
			}
	}

	/**
	 * This method will execute query which fetch data from beeline table and load into
	 * directory.
	 * 
	 * @param queryForOverWrite
	 *            - Statement to fetch data.
	 *  @param externalPath 
	 *  		  - directory where the data needs to be inserted.
	 *  @param databaseName
	 *  		  - database name from where the data needs to be extracted.
	 *  @param startDate
	 *            - to pull data ranging from startDate.
	 *  @param endDate
	 *  		  - to pull data ranging to endDate.
	 *
	 * @throws Exception
	 * 
	 */
	private void executeQueryForOverWrite(String readQueryFile,
			String beelineExternalPath, String beelineDBName) {
		StringBuilder sbInsQuery = new StringBuilder();
		Statement stmt = null;
		
		try {
			stmt = conn.createStatement();
			
			/*sbInsQuery.append(INSERT_OVERWRITE_DIR_COMMAND).append(SPACE).append("'").append(beelineExternalPath).append("'")
			.append(SPACE).append(ROW_FORMAT_DELIMITED).append(SPACE).append(FIELDS_TERMINATED).append(SPACE).append("','").append(SPACE).append("SELECT name,state FROM testbeecustomerdb.testbeecustomertab;");
			*/
			
			sbInsQuery.append(INSERT_OVERWRITE_DIR_COMMAND).append(SPACE).append("'").append(beelineExternalPath).append("'")
			.append(SPACE).append(ROW_FORMAT_DELIMITED).append(SPACE).append(FIELDS_TERMINATED).append(SPACE).append("','").append(SPACE).append(readQueryFile);
			
			logger.info("Insert Query is : " + sbInsQuery.toString());
			
			String query = sbInsQuery.toString().trim();
			System.out.println("Query Formed is : "+query);
			
			stmt.execute(query);
			logger.info("Insert into directory successfull.");
			
		} catch (SQLException e) {
			logger.info("SQL Exception occured for inserting data", e);
		} finally {
			if (stmt != null)
				try {
					stmt.close();
				} catch (SQLException e) {
					logger.info("SQL Exception occured for setting Queue name method while closing statement", e);
				}
			if (sbInsQuery != null)
				sbInsQuery =  null;
		}
	}

	/**
	 * This method will set Queue to execute Hive query.
	 * 
	 * @param queryForQueue
	 *            - Statement to set queue for hive.
	 * @throws Exception
	 * 
	 */
	private void executeQueryForQueue(String queryForQueue) {
		logger.info("Query String for queueName is : " + queryForQueue);
		Statement stmt = null;
		
		try {
			stmt = conn.createStatement();
			stmt.execute(queryForQueue);
			logger.info("Setting queue name successfull.");
		} catch (SQLException e) {
			logger.info("SQL Exception occured for setting Queue name", e);
		} finally {
			if (stmt != null){
				try {
					stmt.close();
				} catch (SQLException e) {
					logger.info("SQL Exception occured for setting Queue name method while closing statement", e);
				}
			}
		}
		
		
	}

	/**
	 * This method will establish connection to Client for first time.
	 * 
	 * @param hiveServer2Jdbc
	 *            - JDBC Connection URL
	 * @param databaseName
	 *            - Hive Database to be used.
	 * @param userName
	 *            - Username to connect.
	 * @param password
	 *            - Password for Username.
	 * 
	 * @throws Exception 
	 * 
	 */
	private void createConnection(String hiveServer2Jdbc, String userName,
			String password, String beelineDBName) {
		try {
			Class.forName(HiveServer2driverName);
			
			conn = DriverManager.getConnection( hiveServer2Jdbc + beelineDBName, userName, password);
			
			logger.info("Connection established !");
			
		} catch (SQLException se) {
			logger.info("SQL Exception occured while establishing connection", se);
		} catch (ClassNotFoundException ce) {
			logger.info("Class Not found exception occured while establishing connection", ce);
		}
	}
	

}
