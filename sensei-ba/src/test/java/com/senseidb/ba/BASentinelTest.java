package com.senseidb.ba;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.net.URLConnection;

import junit.framework.TestCase;

import org.json.JSONObject;

import com.senseidb.util.SingleNodeStarter;

public class BASentinelTest  extends TestCase {

  @Override
  protected void tearDown() throws Exception {
    SingleNodeStarter.shutdown(); 
  }
  @Override
  protected void setUp() throws Exception {    
    File ConfDir1 = new File(BASentinelTest.class.getClassLoader().getResource("ba-conf").toURI());
    
    SingleNodeStarter.start(ConfDir1, 15000);
  }
  public void test1() throws Exception {
  String req = "{" + 
  		"  " + 
  		"    \"from\": 0," + 
  		"    \"size\": 10,\n" + 
  		"    \"selections\": [" + 
  		"    {" + 
  		"        \"terms\": {" + 
  		"            \"color\": {" + 
  		"                \"values\": [\"gold\"]," + 
  		"                \"excludes\": []," + 
  		"                \"operator\": \"or\"" + 
  		"            }" + 
  		"        }" + 
  		"    }" + 
  		"   ]," +
  		"    \"facets\": {\n" + 
  		"        \"category\": {\n" + 
  		"            \"max\": 10,\n" + 
  		"            \"minCount\": 1,\n" + 
  		"            \"expand\": false,\n" + 
  		"            \"order\": \"hits\"\n" + 
  		"        }\n" + 
  		"    }" + 
  		"}";
    
   JSONObject resp = search(new URL("http://localhost:8076/sensei"), new JSONObject(req).toString());
   assertEquals("numhits is wrong", 1110, resp.getInt("numhits"));
}
public static JSONObject search(URL url, String req) throws Exception {
  URLConnection conn = url.openConnection();
  conn.setDoOutput(true);
  BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(conn.getOutputStream(), "UTF-8"));
  String reqStr = req;
  System.out.println("req: " + reqStr);
  writer.write(reqStr, 0, reqStr.length());
  writer.flush();
  BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream(), "UTF-8"));
  StringBuilder sb = new StringBuilder();
  String line = null;
  while((line = reader.readLine()) != null)
    sb.append(line);
  String res = sb.toString();
  // System.out.println("res: " + res);
  JSONObject ret = new JSONObject(res);
  if (ret.opt("totaldocs") !=null){
   // assertEquals(15000L, ret.getLong("totaldocs"));
  }
  return ret;
}
}
