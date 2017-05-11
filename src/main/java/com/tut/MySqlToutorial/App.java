package com.tut.MySqlToutorial;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Hello world!
 *
 */
public class App {
	public static void main(String[] args) throws ClassNotFoundException,
			SQLException, IOException {

		// Function to insert values
//		BufferedReader reader = loadFile("tweets.txt");
//		List<String> tweets=new ArrayList<String>();
//		int i = 1;
//		String line;
//		while ((line = reader.readLine()) != null) {
//			tweets.add(line);
//			System.out.println("inserted line " + i + " " + line);
//			i++;
//		}
//		createDb("test");
		
		//line = "{test:1232324,local:123}";
//		insertIntoTweets(tweets);
		
		//FUNCTION TO DELETE RECORD FROM DBTBL
	//	delFromTweets("tweets.txt");
		
		// FUNCTION TO GET TWEETS
	//	 getTweets();

	}

	private static void delFromTweets(String tweet)
			throws ClassNotFoundException, SQLException, FileNotFoundException {
		Connection con = getDbConnection();
		PreparedStatement preparedStatement = null;
		if (con != null) {
			
			String sql = "DELETE FROM tweets WHERE tweet = ?";
			
			try {
				
				preparedStatement = con.prepareStatement(sql);
				preparedStatement.setString(1, tweet);
				
				// execute delete SQL stetement
				preparedStatement.executeUpdate();
				
				System.out.println("Record is deleted!");

			} catch (Exception e) {
				// TODO: handle exception
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
				}

				if (con != null) {
					con.close();
				}
			}
		}
	}
	
	//FUNCTION TO CREATE DATABSE
	private static void createDb(String DbName) {
		final String DB_URL = "jdbc:mysql://localhost/";
		Connection conn = null;
		   Statement stmt = null;
		   try {
			// STEP 2: Open a connection
				 conn = DriverManager.getConnection(DB_URL, "", "");
				System.out.println("creating database...");
			      stmt = conn.createStatement();
			      String sql = "CREATE DATABASE "+DbName;
			      stmt.executeUpdate(sql);
			      System.out.println("Database created successfully...");
		} catch (SQLException se) {
			se.printStackTrace();
		}
		   catch (Exception e) {
			   e.printStackTrace();
		}
		
	}

	// FUNCTION to insert into MySql dbColom
	private static void insertIntoTweets(List<String> tweets)
			throws ClassNotFoundException, SQLException, FileNotFoundException {
		Connection con = getDbConnection();
		if (con != null) {
			PreparedStatement preparedStatement = null;

			try {
				String sql = "INSERT INTO tweets(`tweet`) VALUES (?)";
				preparedStatement = con.prepareStatement(sql);
				for(String tweet:tweets){
					preparedStatement.setString(1,tweet);
					preparedStatement.addBatch();
				}					
									
					preparedStatement.executeBatch();

				System.out.println("Record is inserted into DBUSER table!");

			} catch (Exception e) {
				// TODO: handle exception
			} 
		}
	}

	private static void getTweets() throws ClassNotFoundException, SQLException {

		Connection con = getDbConnection();
		if (con != null) {
			PreparedStatement preparedStatement = null;
			ResultSet result = null;
			String sql = "select * from tweets";
			try {
				preparedStatement = con.prepareStatement(sql);
				result = preparedStatement.executeQuery();
				while (result.next()) {
					System.out.println("id :" + result.getInt("id"));
					System.out.println("tweet :" + result.getString("tweet"));
				}

			} catch (Exception e) {
				// TODO: handle exception
			} finally {
				if (con != null) {
					con.close();
				}
			}
		}
	}

	private static Connection getDbConnection() throws ClassNotFoundException,
			SQLException {

		final String DB_URL = "jdbc:mysql://localhost/test";

		// STEP 2: Open a connection
		System.out.println("Connecting to database...");
		Connection conn = DriverManager.getConnection(DB_URL, "", "");
		return conn;
	}

	// Function to LOAD FILE DATA
	public static BufferedReader loadFile(String file)throws FileNotFoundException {

		// for reading files from root dorectory
		InputStream in = new FileInputStream(new File(file));
		BufferedReader reader = new BufferedReader(new InputStreamReader(in));
		return reader;
	}
}
