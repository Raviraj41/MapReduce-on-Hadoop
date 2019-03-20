/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mymapreduce;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
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
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author nupurparashar
 */
public class MyReducer {
    
    public static void reducer()
    {
       
        Connection con = null;
        Statement st = null;
        ResultSet rs = null;

    String url = "jdbc:mysql://localhost/mydb";
    String user = "root";
    String password = "nupur";
    List<DBObject> docs = new ArrayList<>();

    try {
        Class.forName("com.mysql.jdbc.Driver");
        con = DriverManager.getConnection(url, user, password);
        st = con.createStatement();
        rs = st.executeQuery("SELECT year, sum(ValueInYen) as ValueInYen\n" +
                                    "FROM `mydb`.`mapperoutput` group by year;");

        while (rs.next()) {//get first result
            
            //System.out.println(rs.getString(1));//coloumn 1
            
            DBObject objDB = new BasicDBObject();
     
            //data for object d3
            objDB.put("Year", rs.getString("year"));
            objDB.put("ValueInYen",rs.getString("ValueInYen"));
            
            docs.add(objDB);
                     //insert list docs to collection
        }
        saveToMongo(docs);

    } catch (SQLException ex) {
        Logger lgr = Logger.getLogger("Reducer()");
        lgr.log(Level.SEVERE, ex.getMessage(), ex);

    }   catch (ClassNotFoundException ex) {
            Logger.getLogger(MyReducer.class.getName()).log(Level.SEVERE, null, ex);
        }   finally {
        try {
            
            if (st != null) {
                st.close();
            }
            if (con != null) {
                con.close();
            }

        } catch (SQLException ex) {
            Logger lgr = Logger.getLogger("Reducer");
            lgr.log(Level.WARNING, ex.getMessage(), ex);
        }
    }
    }
    
    
    public static void saveToMongo(List<DBObject> objectDB)
    {
        Mongo mongo = new MongoClient("localhost", 27017);
	DB db = mongo.getDB("mydb");
      
     ////fetch the collection object ,car is used here,use your own 
            DBCollection coll = db.getCollection("FirstAnalysis");
          
            coll.insert(objectDB);
       
    }
}
