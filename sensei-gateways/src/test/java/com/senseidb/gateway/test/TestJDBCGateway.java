package com.senseidb.gateway.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

import junit.framework.TestCase;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.math.stat.inference.TestUtils;
import org.json.JSONObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import proj.zoie.api.DataConsumer;
import proj.zoie.api.ZoieException;
import proj.zoie.impl.indexing.StreamDataProvider;
import proj.zoie.impl.indexing.ZoieConfig;

import com.senseidb.gateway.SenseiGateway;
import com.senseidb.gateway.test.jdbc.ResultSetJsonFilter;
import com.senseidb.plugin.SenseiPluginRegistry;

public class TestJDBCGateway {

  static File confFile = new File("src/test/resources/configs/jdbc-gateway.properties");
  static File dataFile = new File("src/test/resources/test.json");

  static List<JSONObject> readData(File file) throws Exception {
    LinkedList<JSONObject> dataList = new LinkedList<JSONObject>();
    BufferedReader reader = new BufferedReader(new FileReader(file));
    while (true) {
      String line = reader.readLine();
      if (line == null)
        break;
      dataList.add(new JSONObject(line));
    }
    return dataList;
  }

  static List<JSONObject> dataList;

  static {
    try {
      dataList = readData(dataFile);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  static Configuration config = null;

  static Connection conn = null;
  
  static String insertSql = "insert into test (json,version) values(?,?)";
  
  static String createTableSql = "create table test (json varchar(25600),version int)";
  
  static String dropTableSql = "drop table test";

  static SenseiGateway gateway;
  static SenseiPluginRegistry pluginRegistry;
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
    for (JSONObject obj : dataList){
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
    

    // construct gateway
    
    pluginRegistry = SenseiPluginRegistry.build(config);
    pluginRegistry.start();

    gateway = pluginRegistry.getBeanByFullPrefix("sensei.gateway", SenseiGateway.class);

  }

  @AfterClass
  public static void shutdown() {

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
    
    final LinkedList<JSONObject> jsonList = new LinkedList<JSONObject>();
    
    dataProvider.setDataConsumer(new DataConsumer<JSONObject>(){

      private volatile String version;
      @Override
      public void consume(
          Collection<DataEvent<JSONObject>> events)
          throws ZoieException {

        for (DataEvent<JSONObject> event : events){
          JSONObject jsonObj = event.getData();
          jsonList.add(jsonObj);
          version = event.getVersion();
        }
       
      }

      @Override
      public String getVersion() {
        return version;
      }

      @Override
      public Comparator<String> getVersionComparator() {
        return ZoieConfig.DEFAULT_VERSION_COMPARATOR;
      }
      
    });
    dataProvider.start();
    
    while(true){
      Thread.sleep(500);
      if (jsonList.size()==dataList.size()){
        dataProvider.stop();
        for (int i =0;i<jsonList.size();++i){
          String s1 = jsonList.get(i).getString("id");
          String s2 = dataList.get(i).getString("id");
          TestCase.assertEquals(s1, s2);
        }
        break;
      }
    }
  }
}
