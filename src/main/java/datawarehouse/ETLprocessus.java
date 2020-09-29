package datawarehouse;

import java.sql.SQLException;
import java.util.Hashtable;

public class ETLprocessus {
	public Hashtable<String, String> ETL=null;
	public ETLprocessus() {
		 ETL= new Hashtable<String, String>();
		
		
		ETL.put("transExT","create table if not exists stock.trans as (select * from public.trans where trans is not null);"
				);
		ETL.put("loanExT","create table if not exists stock.loan as (select * from public.loan where loan is not null);"
				+ "");
		ETL.put("orderExT","create table if not exists stock.order as (select * from public.order where \"order\" is not null);"
				);
		ETL.put("districtExT","create table if not exists stock.district as (select * from public.district );"
				);
		ETL.put("accountExT","create table if not exists stock.account as (select * from public.account where account is not null);"
				);
		ETL.put("clientExT","create table if not exists stock.client as (select * from public.client where client is not null);"
				);
		ETL.put("dispExT","create table if not exists stock.disp as (select * from public.disp where disp is not null);"
				);
		
		ETL.put("cardExT","create table if not exists stock.card as (select * from public.card where card is not null);"
				);
		
		ETL.put("update","alter table stock.trans add primary key (trans_id);"
				+ "alter table stock.loan add primary key (loan_id);"
				+ "alter table stock.order add primary key (order_id);"
				+ "alter table stock.district add primary key (district_id);"
				+ "alter table stock.account add primary key (account_id);" 
				+ "alter table stock.account add foreign key (district_id) references stock.district(district_id);"
				+ "alter table stock.client add primary key (client_id);"
				+ "alter table stock.client add foreign key (district_id) references stock.district(district_id);"
				+ "alter table stock.disp add primary key (disp_id);"
				+ "alter table stock.disp add foreign key (account_id) references stock.account(account_id);"
				+ "alter table stock.disp add foreign key (client_id) references stock.client(client_id);"
				+ "alter table stock.card add primary key (card_id);"
				+ "alter table stock.card add foreign key (disp_id) references stock.disp(disp_id);"
				+ "alter table stock.loan add foreign key (account_id) references stock.account(account_id);"
				+ "alter table stock.trans add foreign key (account_id) references stock.account(account_id);"
				+ "alter table stock.order add foreign key (account_id) references stock.account(account_id);");
		
		
		/*
		
		EFL.put("transDim", "create table trans as (select * from trans where trans is not null);\r\n" + 
				"alter table transdim add primary key (trans_id);");
		EFL.put("orderDim", "create table orderDim as (select order_id,bank_to,account_to,amount,k_symbol from \"order\");\r\n" + 
				"alter table orderdim add primary key (order_id);");
		EFL.put("loanDim", "create table loanDim as (select loan_id,amount,duration,payments,status from loan);\r\n" + 
				"alter table loandim add primary key (loan_id);");
		*/
		ETL.put("dateDim", "create table if not exists dwh.dateDim as with Itemdate as (Select account.date as date from stock.account union select\r\n" + 
				"brith_date as date from stock.client union select loan.date as date from stock.loan union \r\n" + 
				"				 select trans.date as date from stock.trans) Select date,\r\n" + 
				"extract( month from date) as mois, extract( year from date) as année ,EXTRACT(day from date) as jour From ItemDate\r\n" + 
				"Order by date;\r\n" + 
				"Alter table dwh.dateDim add column date_id serial primary key;");
		ETL.put("districtDim", "create table if not exists dwh.districtDim as (select * from stock.district);\r\n" + 
				"alter table dwh.districtdim add primary key (district_id);");
		
		ETL.put("informationDim","create table if not exists dwh.informationDim as select cl.client_id as info_id, ac.account_id ,gender,brith_date,d.type as type_account,c.type as type_card,issued,frequency,ac.date as date_account from stock.disp as d\r\n" + 
				"	full join stock.card as c on d.disp_id = c.disp_id\r\n" + 
				"	 full join stock.client as cl on d.client_id = cl.client_id \r\n" + 
				"	  full join stock.account as ac on d.account_id = ac.account_id\r\n" + 
				"	order by d.disp_id;\r\n" + 
				"alter table dwh.informationDim add primary key (info_id) ;");
		
		
		
		
		
		ETL.put("loadFactLoan","create table if not exists dwh.loanFact as select l.loan_id ,infor.info_id,date_id,ac.district_id,amount,duration,payments,status\r\n" + 
				"from stock.loan as l \r\n" + 
				"inner join stock.account as ac on l.account_id = ac.account_id \r\n" + 
				"inner join dwh.dateDim as dm on dm.date = l.date\r\n" + 
				"inner join dwh.districtDim as dis on ac.district_id = dis.district_id\r\n" +
				"inner join dwh.informationDim as infor on infor.account_id = ac.account_id \r\n" +
				";");
		ETL.put("loadFactTrans","create table if not exists dwh.transFact as select t.trans_id ,infor.info_id,date_id,dis.district_id,\"type\",balance,operation,k_symbol,amount,bank,account\r\n" + 
				"from stock.trans as t \r\n" + 
				"inner join stock.account as ac on t.account_id = ac.account_id \r\n" + 
				"inner join dwh.datedim as dm on dm.date = t.date\r\n"+ 
				"inner join dwh.districtDim as dis on ac.district_id = dis.district_id\r\n" +
				"inner join dwh.informationDim as infor on infor.account_id = ac.account_id " +
				";");
		ETL.put("loadFactOrder","create table if not exists dwh.orderFact as select ord.order_id ,infor.info_id,dis.district_id,bank_to,account_to,amount,k_symbol\r\n" + 
				"from stock.order as ord\r\n" + 
				"inner join stock.account as ac on ord.account_id = ac.account_id\r\n" +
				"inner join dwh.districtDim as dis on ac.district_id = dis.district_id \r\n"+
				"inner join dwh.informationDim as infor on infor.account_id = ac.account_id " +
				"\r\n" + 
				";");
		
		ETL.put("updateFact","alter table dwh.loanFact add primary key(loan_id,info_id,date_id,district_id);"
				+ "alter table dwh.transFact \r\n" + 
				"add primary key(trans_id,info_id,date_id,district_id); \r\n"
				+ "alter table dwh.orderFact add primary key(order_id,info_id,district_id);");
		
	}
	//pour l'extraction des tabales
	public void ExtractAndTronsform(ConnectToPostgres connect){
		try {
			
			connect.sendUpdate(ETL.get("transExT"));
			connect.sendUpdate(ETL.get("loanExT"));
			connect.sendUpdate(ETL.get("orderExT"));
			connect.sendUpdate(ETL.get("districtExT"));
			connect.sendUpdate(ETL.get("accountExT"));
			connect.sendUpdate(ETL.get("clientExT"));
			connect.sendUpdate(ETL.get("dispExT"));
			connect.sendUpdate(ETL.get("cardExT"));
			connect.sendUpdate(ETL.get("update"));
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}


	//pour charge les donnees 
	public void Load(ConnectToPostgres connect){
		try {
			connect.sendUpdate(ETL.get("informationDim"));
			connect.sendUpdate(ETL.get("dateDim"));
			connect.sendUpdate(ETL.get("districtDim"));
			connect.sendUpdate(ETL.get("loadFactLoan"));
			connect.sendUpdate(ETL.get("loadFactOrder"));
			connect.sendUpdate(ETL.get("loadFactTrans"));
			connect.sendUpdate(ETL.get("updateFact"));
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
