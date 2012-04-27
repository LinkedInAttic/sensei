package com.senseidb.search.client;

import org.json.JSONObject;

import com.senseidb.search.client.json.JsonSerializer;
import com.senseidb.search.client.req.Facet;
import com.senseidb.search.client.req.Selection;
import com.senseidb.search.client.req.SenseiClientRequest;

public class Test {
  public static void main(String[] args) throws Exception {
    SenseiServiceProxy senseiServiceProxy = new SenseiServiceProxy("localhost", 8080);
    //SenseiClientRequest clientRequest = SenseiClientRequest.builder().addFacet("account_id", Facet.builder().minHit(0).max(200).build()).build();
    SenseiClientRequest clientRequest = SenseiClientRequest.builder().filter(Selection.terms("account_id", "1139")).paging(1000, 10).addFacet("account_id", Facet.builder().minHit(0).max(200).build()).build();
    
    
    JSONObject json = (JSONObject) JsonSerializer.serialize(clientRequest);
    JSONObject mapReduce = new JSONObject().put("function", "sensei.distinctCount").put("parameters", new JSONObject().put("column", "account_id"));
    json.put("mapReduce", mapReduce);
    System.out.println(new JSONObject(senseiServiceProxy.sendPostRaw(senseiServiceProxy.getSearchUrl(), json.toString())).toString(1));
  }
}
