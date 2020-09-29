package datawarehouse;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class ConnectToPostgres {
	
	public String url = "";
	public String utilisateur = "";
	public String motDePasse = "";
	public Connection connexion = null;
	public Statement statement = null;
	
	//connect to data base 
	public ConnectToPostgres(String url,String utilisateur,String motDePasse) {
		this.url = url;
		this.utilisateur=utilisateur;
		this.motDePasse=motDePasse;
		
		try {
			
			connexion = DriverManager.getConnection(url, utilisateur, motDePasse);
			
			
			
		} catch (Exception e) {
			// TODO: handle exception
		}
	}
	
	//get connection
	public  Connection ConnecttoDataBase() {return connexion;}
	
	// close connection to data base
	public void closeConnection() {
		if(connexion != null) {
			try {
					connexion.close();
					System.out.print("close connection");
					
			} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			}	
		}
	}
	
	// send query to data base
	public ResultSet sendQuery(String yourQuery) throws SQLException {
		statement = connexion.createStatement();
		return statement.executeQuery(yourQuery);
	}
	
	public int sendUpdate(String yourUpdate) throws SQLException {
		statement = connexion.createStatement();
		return statement.executeUpdate(yourUpdate);
	}
}
