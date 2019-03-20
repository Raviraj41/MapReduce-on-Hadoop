/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mymapreduce;
import com.mysql.jdbc.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.lang.Package;
import java.lang.*;

/**
 *
 * @author nupurparashar
 */
public class MyMapper {
    private static void createTable() {
   
    }
    public static void mapper()
    {
    Connection con = null;
        java.sql.CallableStatement cs = null;
    //ResultSet rs = null;
    
    
     
    String url = "jdbc:mysql://localhost/mydb";
    String user = "root";
    String password = "nupur";
 
    try {
        
        Class.forName("com.mysql.jdbc.Driver");
        con = DriverManager.getConnection(url, user, password);
        
        cs = con.prepareCall("{call mapperClassProc}");
        cs.executeQuery();
        //st2.executeQuery(myTableName);
        
        System.out.println("Table Created in mysql under mydb");
        //createTable();
        //while (rs.next()) {//get first result
         
        //}

    } catch (SQLException ex) {
        Logger lgr = Logger.getLogger("Mapper()");
        lgr.log(Level.SEVERE, ex.getMessage(), ex);

    }   catch (ClassNotFoundException ex) {
            Logger.getLogger(MyMapper.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
        try {
            
            if (cs != null) {
                cs.close();
            }
            if (con != null) {
                con.close();
            }

        } catch (SQLException ex) {
            Logger lgr = Logger.getLogger("Mapper");
            lgr.log(Level.WARNING, ex.getMessage(), ex);
        }
    }
    }
    
}