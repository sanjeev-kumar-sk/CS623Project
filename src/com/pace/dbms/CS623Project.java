package com.pace.dbms;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class CS623Project {
	
    private Connection conn = null;
    
    public CS623Project() {
    	
    	this.setConnection();
    	
    }
    
    public Connection getConn() {
		return conn;
	}

	public void setConnection() {
    	
    	try {
    		
    		Class.forName("org.postgresql.Driver");
    		conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/postgres", "postgres", "postgres");
    		
    		// For atomicity
    		this.getConn().setAutoCommit(false);
    		
    		// For Isolation
    		this.getConn().setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
	    } 
    	catch (Exception e) {
			System.out.println("An exception was thrown while setting up connection to postgres");
	        e.printStackTrace();
	    }
    }
	
	public void createTables() throws SQLException {
		 
		Statement stmt = null;
		
		try {
			
			stmt= conn.createStatement();
			String createTableProd = "create table Product(prod_id CHAR(10), pname VARCHAR(30), price decimal)";
			String createTableDepot= "create table Depot(dep_id char(20),address varchar(25),volume integer)";
			String createTableStock= "create table Stock(prod_id char(20),dep_id char(20),quantity integer)";
			stmt.executeUpdate(createTableProd);
			stmt.executeUpdate(createTableDepot);
			stmt.executeUpdate(createTableStock);
			
		} 
		catch (SQLException e) {
			System.out.println("An exception was thrown while creating tables");
			e.printStackTrace();
			
			// For atomicity
			conn.rollback();
			stmt.close();
		}
		
		conn.commit();
		System.out.println("Tables Created ");
		
	}
	
	public void insertValues() throws SQLException {
		
		Statement query = null;
		
		try {

			query = conn.createStatement();
		
			query.execute("insert into product values('P1', 'tape', '2.5')");
			query.execute("insert into product values('P2', 'TV', '250')");
			query.execute("insert into product values('P3', 'VCR', '80')");
			
			query.execute("insert into depot values('D1', 'New York', 9000)");
			query.execute("insert into depot values('D2', 'Syracuse',6000)");
			query.execute("insert into depot values('D3', 'New York',2000)");
			
			query.execute("insert into stock values('P1', 'D2',1000)");
			query.execute("insert into stock values('P1', 'D3',1200)");
			query.execute("insert into stock values('P3', 'D1',3000)");
			query.execute("insert into stock values('P3', 'D3',2000)");
			query.execute("insert into stock values('P2', 'D3',1500)");
			query.execute("insert into stock values('P2', 'D1',-400)");
			query.execute("insert into stock values('P2', 'D2',2000)");

			query.close();
			
		} catch (SQLException e) {
			System.out.println("An exception was thrown when inserting values");
			e.printStackTrace();
			this.getConn().rollback();
			query.close();
		}
		
		this.getConn().commit();
		
	}
	
	public void addContraints() throws SQLException {


		Statement query = null;
		
		try {
			query = this.getConn().createStatement();
			String alterTableProd = "alter table Product add constraint pk_product  primary key(prod_id)";
			String alterTableDepot = "alter table depot add constraint pk_depot primary key(dep_id)";
			String alterTableStock = "alter table stock add constraint pk_stock primary key(prod_id,dep_id)";
			String fkProdStock = "alter table stock add constraint fk_stock_product foreign key(prod_id) references product(prod_id) ON DELETE CASCADE";
			String fkDepotStock = "alter table stock add constraint fk_stock_depot foreign key(dep_id) references depot(dep_id) ON DELETE CASCADE";
			query.execute(alterTableProd);
			query.execute(alterTableDepot);
			query.execute(alterTableStock);
			query.execute(fkProdStock);
			query.execute(fkDepotStock);

		} 
		catch (SQLException e) {
			System.out.println("An exception was thrown when adding constraint");
			e.printStackTrace();
			
			// For atomicity
			this.getConn().rollback();
			query.close();
			conn.close();
		}
		this.getConn().commit();
		System.out.println("Constraints added");
		
	}
	
	public void doTransactions() throws SQLException {
		
		Statement query = null;
		try {
			query = this.getConn().createStatement();
			query.execute("delete from depot where dep_id='D1'");
		}
		catch (SQLException e) {
			System.out.println("An exception was thrown while doing transactions");
			e.printStackTrace();
			
			// For atomicity
			conn.rollback();
			query.close();
			conn.close();
			return;
		}
		this.getConn().commit();
		System.out.println("Values Deleted");
		query.close();
	}

    
	public static void main(String args[]) {
		
		CS623Project project_demo = new CS623Project();
		
		try {
			project_demo.createTables();
			project_demo.insertValues();
			project_demo.addContraints();
			project_demo.doTransactions();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("Got exception while execution");
		}
		
	}
}
