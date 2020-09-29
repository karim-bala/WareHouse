package datawarehouse;

import java.awt.EventQueue;

import javax.swing.JFrame;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.border.EmptyBorder;
import javax.swing.UIManager;
import javax.swing.JButton;
import javax.swing.JFileChooser;
import javax.swing.JTextField;
import java.awt.event.ActionListener;
import java.io.File;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.Hashtable;
import java.awt.event.ActionEvent;
import javax.swing.JLabel;

public class GUserInterface extends JFrame {

	public static String URL="";
	public static String USER="";
	public static String PASSWORD="";
	public ConnectToPostgres connect =null; 
	
	public static String getURL() {
		return URL;
	}

	public static void setURL(String uRL) {
		URL = uRL;
	}

	public static String getUSER() {
		return USER;
	}

	public static void setUSER(String uSER) {
		USER = uSER;
	}

	public static String getPASSWORD() {
		return PASSWORD;
	}

	public static void setPASSWORD(String pASSWORD) {
		PASSWORD = pASSWORD;
	}

	
	
	private JPanel contentPane;
	private JTextField textField;
	private JTextField textField_1;
	private JTextField textField_2;
	private JTextField textField_3;

	/**
	 * Launch the application.
	 */
	public static void main(String[] args) {
		EventQueue.invokeLater(new Runnable() {
			public void run() {
				try {
					GUserInterface frame = new GUserInterface();
					frame.setVisible(true);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}

	/**
	 * Create the frame.
	 */
	public GUserInterface() {
		setTitle("DataWareHouse");
		final Hashtable<String, String> pathTable = new Hashtable<String, String>();
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		setBounds(100, 100, 450, 300);
		contentPane = new JPanel();
		contentPane.setToolTipText("");
		contentPane.setForeground(UIManager.getColor("Button.darkShadow"));
		contentPane.setBackground(UIManager.getColor("Button.background"));
		contentPane.setBorder(new EmptyBorder(5, 5, 5, 5));
		setContentPane(contentPane);
		contentPane.setLayout(null);
		/////////////////////////////////////////////////////////////////////////////////////////////////
		JPanel panel = new JPanel();
		panel.setBounds(0, 0, 434, 261);
		contentPane.add(panel);
		panel.setLayout(null);
		/////////////////////////////////////////////////////////////////////////////////////////////////
		JButton btnNewButton = new JButton("choose file");
		btnNewButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				
				
				JFileChooser fc = new JFileChooser();
				fc.setMultiSelectionEnabled(true);
				int response = fc.showOpenDialog(contentPane);
				if (response == JFileChooser.APPROVE_OPTION)  {
					File[] fs = fc.getSelectedFiles();
					
					System.out.println("Nom des fichier sélectionés:\n");
					
					String allFiles ="";
					
					for(File f:fs) {
						pathTable.put(f.getName().toString().replace(".csv", ""), f.toString());
						allFiles += f.getName().toString()+" == ";
						
					}
					System.out.println("size de path : "+pathTable.size());
					/*Enumeration names = pathTable.keys();
			
					   while(names.hasMoreElements()) {
					      key = (String) names.nextElement();
					      System.out.println("Key: " +key+ " & Value: " +
					      pathTable.get(key));
					      
					   }
					  */
					   textField.setText(allFiles);
					
				}else {
					System.out.println("The file open operation was cancelled.");
					
				}
			}
		});
		btnNewButton.setToolTipText("");
		btnNewButton.setBounds(45, 29, 89, 23);
		panel.add(btnNewButton);
		//////////////////////////////////////////////////////////////////////////////////////////////
		textField = new JTextField();
		textField.setBounds(162, 30, 251, 20);
		panel.add(textField);
		textField.setColumns(10);
		//////////////////////////////////////////////////////////////////////////////////////////////   create data base
		JButton btnNewButton_1 = new JButton("create data base ");
		btnNewButton_1.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				if (pathTable.size() != 8) {
					JOptionPane.showMessageDialog(contentPane, ""
							+ "nomber des fichier deffirent de != 8", "ERROR", JOptionPane.ERROR_MESSAGE);
				}else {
					readFromCSV read_files = new readFromCSV();
					createTables TABLES =new createTables();
					String key="";
					int i =0;
					Enumeration<String> names = pathTable.keys();
					
					   while(names.hasMoreElements()) {
					      key = names.nextElement();
					      
					     
					      
					      if (TABLES.tables.containsKey(key) == true) {
					      read_files.readFileCsv(connect,pathTable.get(key), key, TABLES.tables.get(key));
					      }else {
					    	  JOptionPane.showMessageDialog(contentPane, ""
					    	  		+ "table name not exist !", "ERROR", JOptionPane.ERROR_MESSAGE);
								  i = i+1;
					      }
					      
					   }
					   if(i != 0) {
						   JOptionPane.showMessageDialog(contentPane, ""
					    	  		+ "table name not exist !", "ERROR", JOptionPane.ERROR_MESSAGE);
								  
					   }else {
						   names = TABLES.update.keys();
						   boolean err = true;
						   while(names.hasMoreElements()) {
							      key = names.nextElement();
							      try {
									read_files.updatetable(connect,TABLES.update.get(key));
								} catch (SQLException e1) {
									// TODO Auto-generated catch block
									e1.printStackTrace();
									err = false;
								}
						  }
						  if(err) {
							  JOptionPane.showMessageDialog(contentPane, ""
									+ "base des donnees est bien creer", "OK", JOptionPane.INFORMATION_MESSAGE);
							 } 
					   }
				}
			}
		});
		btnNewButton_1.setBounds(237, 140, 144, 23);
		panel.add(btnNewButton_1);
		////////////////////////////////////////////////////////////////////////////////////////////   create data warehouse
		JButton btnNewButton_2 = new JButton("create data warehouse");
		btnNewButton_2.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				
				try {
				//le processus ETL
				try {
					connect.sendUpdate("drop schema if  exists stock cascade;");
					connect.sendUpdate("create schema if not exists stock;");
				} catch (SQLException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				
				ETLprocessus etl = new ETLprocessus();
				// extraction and trosformation
				etl.ExtractAndTronsform(connect);
				
				// loading 
				try {
					connect.sendUpdate("drop schema if exists dwh cascade;");
					connect.sendUpdate("create schema if not exists dwh;");
				} catch (SQLException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				etl.Load(connect);
				JOptionPane.showMessageDialog(contentPane, ""
						+ "etropot de donnees est bien creer", "OK", JOptionPane.INFORMATION_MESSAGE);
				
				}catch (Exception e1) {
					// TODO: handle exception
				}
				
				
				
				
			}
		});
		btnNewButton_2.setBounds(237, 192, 145, 23);
		panel.add(btnNewButton_2);
		////////////////////////////////////////////////////////////////////////////////////////////////   connect to DB
		JButton btnConnectToDb = new JButton("connect to DB");
		btnConnectToDb.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				URL = textField_1.getText().toString();
				USER = textField_2.getText().toString();
				PASSWORD = textField_3.getText().toString();
				
				connect= new ConnectToPostgres(URL,USER,PASSWORD);
				if(connect.ConnecttoDataBase()!= null) {
					JOptionPane.showMessageDialog(contentPane, "Connected to the database!", "Alert", JOptionPane.INFORMATION_MESSAGE);

				}else {
					JOptionPane.showMessageDialog(contentPane, ""
							+ "erreur de connection au serveur postgres", "OK", JOptionPane.ERROR_MESSAGE);
				}
			}
		});
		btnConnectToDb.setBounds(301, 78, 112, 23);
		panel.add(btnConnectToDb);
		///////////////////////////////////////////////////////////////////////////////////////
		textField_1 = new JTextField();
		textField_1.setToolTipText("");
		textField_1.setBounds(10, 79, 86, 20);
		panel.add(textField_1);
		textField_1.setColumns(10);
		///////////////////////////////////////////////////////////////////////////////////////
		textField_2 = new JTextField();
		textField_2.setBounds(106, 79, 86, 20);
		panel.add(textField_2);
		textField_2.setColumns(10);
		///////////////////////////////////////////////////////////////////////////////////////
		textField_3 = new JTextField();
		textField_3.setToolTipText("localhost");
		textField_3.setBounds(205, 79, 86, 20);
		panel.add(textField_3);
		textField_3.setColumns(10);
		////////////////////////////////////////////////////////////////////////////////////////
		JLabel lblLocalhost = new JLabel("Localhost");
		lblLocalhost.setBounds(31, 105, 46, 14);
		panel.add(lblLocalhost);
		////////////////////////////////////////////////////////////////////////////////////////
		JLabel lblUser = new JLabel("User");
		lblUser.setBounds(136, 105, 29, 14);
		panel.add(lblUser);
		////////////////////////////////////////////////////////////////////////////////////////
		JLabel lblPassword = new JLabel("Password");
		lblPassword.setBounds(229, 105, 46, 14);
		panel.add(lblPassword);
	}
}
