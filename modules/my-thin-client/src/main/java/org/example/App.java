package org.example;

import java.sql.*;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void my_test() throws SQLException, ClassNotFoundException {
        Class.forName("org.apache.ignite.IgniteJdbcDriver");
        String url = "";
        url = "jdbc:ignite:thin://127.0.0.1:10800/public?lazy=true";
        //url = "jdbc:ignite:thin://127.0.0.1:10800/public?lazy=true;userToken=wudafu";
        url = "jdbc:ignite:thin://127.0.0.1:10800/public?userToken=wudafu;lazy=true;";
        Connection conn = DriverManager.getConnection(url);
        //Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1:10800/public?lazy=true;");
        //Statement stmt = conn.createStatement();

        PreparedStatement stmt = conn.prepareStatement("SELECT * FROM Categories where CategoryID = ?");

        stmt.setObject(1, 5);

        ResultSet rs = stmt.executeQuery();

        while (rs.next()) {
            String name = rs.getString(1);
            String desction = rs.getString(2);
            System.out.println(name + " " + desction);
        }
        rs.close();
        stmt.close();
        conn.close();
    }

    public static void main( String[] args ) throws SQLException, ClassNotFoundException {
        my_test();
        System.out.println( "Hello World!" );
    }
}




























