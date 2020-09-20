package datawarehouse;

import java.util.Hashtable;

public class createTables {
	static public Hashtable<String, String> tables = null;
	public createTables() {
		tables = new Hashtable<String, String>();
		tables.put("account", "account_id int not null primary key,"
				+ "district_id int,"
				+ "frequency varchar(20),"
				+ "date date");
		tables.put("client","client_id int not null primary key,"
				+ "gender varchar(5),"
				+ "brith_date date,"
				+ "district_id int");
		tables.put("district","district_id int not null primary key,"
				+ "A2 int,"
				+ "A3 int,"
				+ "A4 int,"
				+ "A5 int,"
				+ "A6 int,"
				+ "A7 int,"
				+ "A8 int,"
				+ "A9 int,"
				+ "A10 int,"
				+ "A11 int,"
				+ "A12 int,"
				+ "A13 int,"
				+ "A14 int,"
				+ "A15 int,"
				+ "A16 int");
		tables.put("disp", "disp_id int not null primary key,"
				+ "client_id int,"
				+ "account_id int,"
				+ "type varchar(10)");
		tables.put("trans", "trans_id int not null primary key,"
				+ "account_id int,"
				+ "date date,"
				+ "type varchar(10),"
				+ "operation varchar(20),"
				+ "amount int,"
				+ "balance int,"
				+ "k_symbol varchar(10),"
				+ "bank varchar (10),"
				+ "account int"
				);
		tables.put("order", "order_id int not null primary key,"
				+ "account_id int,"
				+ "bank_to varchar(20),"
				+ "account_to int ,"
				+ "amount decimal,"
				+ "k_symbol varchar(20)");
		tables.put("loan", "loan_id int not null primary key,"
				+ "account_id int ,"
				+ "date date,"
				+ "amount int ,"
				+ "duration int ,"
				+ "payments decimal,"
				+ "status varchar");
		tables.put("card", "card_id int not null primary key,"
				+ "disp_id int ,"
				+ "type varchar(10),"
				+ "issued date");
		
	}
	
}
