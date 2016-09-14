package ssa;

import java.sql.*;
import java.io.*;
import java.util.Properties;

public class Mainline {

    static Connection con = null;
    static Statement st = null;
    static ResultSet rs = null;
    static String fileName = "C:/Users/admin/Desktop/class notes.txt";

    public void closeConnection(Connection con, Statement stmt, ResultSet rs) throws SQLException {
        if (con != null) {
            con.close();
        }

        if (stmt != null) {
            stmt.close();
        }

        if (rs != null) {
            rs.close();
        }
    }

    public void insertData(String sql) throws SQLException {

        try {
            Properties prop = new Properties();
            prop.load(new FileInputStream("dbconnection.properties"));

            String userName = prop.getProperty("userName");
            String pass = prop.getProperty("pass");
            String url = prop.getProperty("url");

            con = DriverManager.getConnection(url, userName, pass);
            st = con.createStatement();
            

            //String sql = "insert into student (first_name, last_name, sat, gpa, major) values ('George', Washington', 1600, 4.0, null)";
            int rowAffected = st.executeUpdate(sql);
            if(rowAffected == 0) {
                System.out.println("****************Insert data was unsuccessful****************");
            }
            //System.out.println(rowAffected);

        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            if (con != null) {
                con.close();
            }
            if (st != null) {
                st.close();
            }

        }

    }

    public void deleteData(String sql) throws SQLException {

        try {
            Properties prop = new Properties();
            prop.load(new FileInputStream("dbconnection.properties"));

            String userName = prop.getProperty("userName");
            String pass = prop.getProperty("pass");
            String url = prop.getProperty("url");

            con = DriverManager.getConnection(url, userName, pass);
            st = con.createStatement();

            //String sql = "";
            int rowAffected = st.executeUpdate(sql);
            if(rowAffected == 0) {
                System.out.println("****************Delete data was unsuccessful****************");
            }
            //System.out.println(rowAffected);

        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            if (con != null) {
                con.close();
            }
            if (st != null) {
                st.close();
            }

        }

    }

    public void updateData(String sql) throws SQLException {

        try {
            Properties prop = new Properties();
            prop.load(new FileInputStream("dbconnection.properties"));

            String userName = prop.getProperty("userName");
            String pass = prop.getProperty("pass");
            String url = prop.getProperty("url");

            con = DriverManager.getConnection(url, userName, pass);
            st = con.createStatement();

            //String sql = "";
            int rowAffected = st.executeUpdate(sql);
            if(rowAffected == 0) {
                System.out.println("****************Update data was unsuccessful****************");
            }
            //System.out.println(rowAffected);

        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            if (con != null) {
                con.close();
            }
            if (st != null) {
                st.close();
            }

        }

    }

    public void selectData(String sql) throws SQLException {

        try {
            Properties prop = new Properties();
            prop.load(new FileInputStream("dbconnection.properties"));

            String userName = prop.getProperty("userName");
            String pass = prop.getProperty("pass");
            String url = prop.getProperty("url");

            con = DriverManager.getConnection(url, userName, pass);
            st = con.createStatement();
            rs = st.executeQuery(sql);
            
            if(!rs.next()) {
                System.out.println("No Student Found");
                closeConnection(con, st, rs);
                System.exit(0);;
            }
                
            rs.beforeFirst();
            
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnsNumber = rsmd.getColumnCount();
            
            for(int i = 1; i <= columnsNumber; i++) {
                System.out.printf("%-15s", rsmd.getColumnName(i));
            }

            while (rs.next()) {
                
                System.out.println();
                for(int i = 1; i <= columnsNumber; i++) {
                    System.out.printf("%-15s", rs.getString(i));
                }
                
            }
            
            System.out.println();
            System.out.println();
            
            
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            closeConnection(con, st, rs);

        }

    }
    
    
    public void backup(String sql) throws SQLException {
        
        try {
            File file = new File(fileName);
            FileWriter fw = new FileWriter(file.getAbsoluteFile());
            BufferedWriter bw = new BufferedWriter(fw);
            
            Properties prop = new Properties();
            prop.load(new FileInputStream("dbconnection.properties"));

            String userName = prop.getProperty("userName");
            String pass = prop.getProperty("pass");
            String url = prop.getProperty("url");

            con = DriverManager.getConnection(url, userName, pass);
            st = con.createStatement();
            rs = st.executeQuery(sql);
            
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnsNumber = rsmd.getColumnCount();
            
            
            
            while(rs.next()) {
                String insertLine = "insert into student(";
                Boolean concatFlag = false;
            
            
            for(int i = 1; i <= columnsNumber; i++) {
                if(concatFlag == true && i == columnsNumber) {
                    insertLine += rsmd.getColumnName(i);
                } else if (concatFlag == false) {
                    if(i == columnsNumber - 1) {
                        concatFlag = true;
                    }
                    insertLine += rsmd.getColumnName(i) + ",";
                }
            }
            
            insertLine += ") values (";
            concatFlag = false;
            
            for(int i = 1; i <= columnsNumber; i++) {
                if(concatFlag == true && i == columnsNumber) {
                    if(rsmd.getColumnTypeName(i) == "INT") {
                        insertLine += rs.getString(i);
                    }
                    
                    if(rsmd.getColumnTypeName(i) == "VARCHAR") {
                        insertLine += "'" + rs.getString(i) + "'";
                    }
                    
                    if(rsmd.getColumnTypeName(i) == "DATE") {
                        insertLine += "'" + rs.getString(i) + "'";
                    }
                    
                    if(rsmd.getColumnTypeName(i) == "DECIMAL") {
                        insertLine += "'" + rs.getString(i) + ",";
                    }
                    
                } else if (concatFlag == false) {
                    if(i == columnsNumber - 1) {
                        concatFlag = true;
                    }
                    
                    if(rsmd.getColumnTypeName(i) == "INT") {
                        insertLine += rs.getString(i) + ",";
                    }
                    
                    if(rsmd.getColumnTypeName(i) == "VARCHAR") {
                        insertLine += "'" + rs.getString(i) + "',";
                    }
                    
                    if(rsmd.getColumnTypeName(i) == "DATE") {
                        insertLine += "'" + rs.getString(i) + "',";
                    }
                    
                    if(rsmd.getColumnTypeName(i) == "DECIMAL") {
                        insertLine += "'" + rs.getString(i) + ",";
                    }
                }
            }
            
            insertLine += ")";
            bw.write(insertLine);
            bw.newLine();
            
            }
                
            
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            closeConnection(con, st, rs);
        }

    }
        
    public static void main(String[] args) throws SQLException {
        // TODO Auto-generated method stub
        Mainline connection1 = new Mainline();
        connection1.insertData("insert into student (id,first_name,last_name,start_date,gpa,sat) values (999, 'George', 'Washington', '2016-10-10', 4.0, 1600)");
        connection1.selectData("select * from student where first_name = 'George' AND last_name = 'Washington';");
        connection1.updateData("update student set gpa = 3.5, sat = 1450, major_id = 1 where id = 999");
        connection1.selectData("select * from student where first_name = 'George' AND last_name = 'Washington';");
        connection1.deleteData("delete from student where last_name = 'Washington' AND sat = 1450;");
        connection1.selectData("select * from student where first_name = 'George' AND last_name = 'Washington';");
//        connection1.backup("select * from student;");
    }

}
