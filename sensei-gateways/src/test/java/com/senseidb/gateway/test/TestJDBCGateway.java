package com.senseidb.gateway.test;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.json.JSONObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.linkedin.zoie.impl.indexing.StreamDataProvider;

import com.senseidb.gateway.SenseiGateway;
import com.senseidb.gateway.test.jdbc.ResultSetJsonFilter;
import com.senseidb.plugin.SenseiPluginRegistry;

public class TestJDBCGateway{

  static File confFile = new File("src/test/resources/configs/jdbc-gateway.properties");

  static SenseiGateway gateway;
  static SenseiPluginRegistry pluginRegistry;

  static Configuration config = null;

  static Connection conn = null;
  
  static String insertSql = "insert into test (json,version) values(?,?)";
  
  static String createTableSql = "create table test (json varchar(25600),version int)";
  
  static String dropTableSql = "drop table test";

  @BeforeClass
  public static void init() throws Exception{
    // connect to db

      config = new PropertiesConfiguration(confFile);
      String userName = config.getString("sensei.gateway.jdbc.username");
      String password = config.getString("sensei.gateway.jdbc.password", null);
      String url = config.getString("sensei.gateway.jdbc.url");
      Class.forName(config.getString("sensei.gateway.jdbc.driver"));
      conn = DriverManager.getConnection(url, userName, password);
      conn.setAutoCommit(false);
      System.out.println("Database connection established");
    
      try{
        PreparedStatement createDBStmt = conn.prepareStatement(createTableSql);
        createDBStmt.execute();
        conn.commit();
      }
      catch(Exception e){
        e.printStackTrace();
      }
      
    // populate data
    for (JSONObject obj : BaseGatewayTestUtil.dataList){
      try{
      PreparedStatement stmt = conn.prepareStatement(insertSql);
      stmt.setString(1,obj.toString());
      stmt.setInt(2, obj.getInt("id")+1);
      stmt.executeUpdate();  
      }
      catch(Exception e){
        e.printStackTrace();
      }
    }
    
    conn.commit();
    
    pluginRegistry = SenseiPluginRegistry.build(config);
    pluginRegistry.start();

    gateway = pluginRegistry.getBeanByFullPrefix("sensei.gateway", SenseiGateway.class);
    

  }

  @AfterClass
  public static void shutdown() {
    gateway.stop();
    pluginRegistry.stop();
    if (conn != null) {
      try {
        try{
          PreparedStatement createDBStmt = conn.prepareStatement(dropTableSql);
          createDBStmt.execute();
          conn.commit();
        }
        catch(Exception e){
          e.printStackTrace();
        }
        conn.close();
        System.out.println("Database connection terminated");
      } catch (Exception e) { /* ignore close errors */
      }
    }
  }

  @Test
  public void testHappyPath() throws Exception {
    final StreamDataProvider<JSONObject> dataProvider = gateway.buildDataProvider(new ResultSetJsonFilter(), String.valueOf("0"), null, null);
    BaseGatewayTestUtil.doTest(dataProvider);
  }
}
