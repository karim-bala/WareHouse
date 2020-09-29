package datawarehouse;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class readFromCSV {
	//ConnectToPostgres connect = new ConnectToPostgres(GUserInterface.URL, GUserInterface.USER, GUserInterface.PASSWORD);

	public void readFileCsv(ConnectToPostgres connect,String csv_file,String table_name,String table_schema) {
		//connecte to data base postgres
				if (connect.ConnecttoDataBase()!= null) {
			
			
			
			// Configuration of file CSV
			String line = "";
			
			// RÃ©cupÃ©ration des types des attributs
			String[] att = table_schema.split(",");
			List<String> attributesTypes = new ArrayList<String>();
			String attributes="";
			for (int i = 0; i < att.length; i++) {
				String[] tmp = att[i].trim().split("\\s+");
				if (tmp.length > 1) {
					attributesTypes.add(tmp[1]);
					attributes+=tmp[0]+",";
				}else
					System.out.println("Attribute syntax error: " + tmp[1]);
			}
			
			try {
				connect.sendUpdate("DROP TABLE IF EXISTS  public."+table_name+" CASCADE;");
				connect.sendUpdate("CREATE TABLE IF NOT EXISTS public." + table_name + " (" + table_schema + ");");
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			// Ouverture du fichier CSV
			try (BufferedReader br = new BufferedReader(new FileReader(csv_file))) {

				// Sauter l'en-tÃªte
				br.readLine();
				//tableau des nombres
				String nombres ="0123456789" ;
				// Lecture du fichier CSV ligne par ligne
				
				while ((line = br.readLine()) != null) {
					
					
					while(line.contains(",,")) {
						line =line.toString().replace(",,", ",null,");
					}	
					
					
					if(line.toString().endsWith(",")) {line =line.toString()+"null";}
					
					
					String query;
					String value="";
					String[] values=line.toString().split(",");
					for (String v : values) {
						
						//reccupére le premier caractre de l'attribue
						char first_cara = v.charAt(0);
						//voir si le premier caractre est nombre ou non
						if(nombres.indexOf(first_cara)!= -1) {
							if(v.contains("/")){
								//si l'attribue est un date
								value += v.replace("\"", "\'")+",";
							}else {
								//si l'attribue est un chiffire
								value += v+",";
							}
						}else {
							//si l'attribue est chaine de caractre
							
							if (v.contentEquals("null")) { value += v+",";}
							else {value += v.replace("\"", "\'")+",";}
						}
						
					}
					
					
					
					// RÃ©cupÃ©rer les valeurs des attributs prÃ©sentes 
					// dans "line" et construire une requÃªte "INSERT" 
					// pour les insÃ©rer dans la table.
					query = "INSERT INTO public."+table_name+"("+attributes.substring(0, attributes.length()-1)+") "
							+ "VALUES ("+value.substring(0,value.length()-1)+");";
					
					try {
						connect.sendUpdate(query);
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				
				System.out.println("Insertion completed");

			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
	}
	
	public void updatetable(ConnectToPostgres connect,String query) throws SQLException {
		connect.sendUpdate(query);
	}
}
