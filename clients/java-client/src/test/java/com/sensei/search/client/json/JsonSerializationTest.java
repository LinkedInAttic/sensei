package com.sensei.search.client.json;


import junit.framework.TestCase;

import org.json.JSONObject;
import org.junit.Test;

import com.sensei.search.client.json.JsonDeserializer;
import com.sensei.search.client.json.req.Facet;
import com.sensei.search.client.json.req.FacetInit;
import com.sensei.search.client.json.req.SelectionContainer;
import com.sensei.search.client.json.req.SenseiClientRequest;
import com.sensei.search.client.json.res.SenseiResult;

public class JsonSerializationTest extends TestCase {
    
    @Test
    public void test1Deserialization() throws Exception {
        String response = new String(IOUtils.getBytes(getClass().getClassLoader().getResourceAsStream("json/senseiresult.json")), "UTF-8");
        SenseiResult senseiResult = JsonDeserializer.deserialize(SenseiResult.class, new JSONObject(response));
        assertEquals(senseiResult.getFacets().size(), 1);
        System.out.println(senseiResult);
    }
    @Test
    public void test2Serialization() throws Exception {       
        SenseiClientRequest senseiRequest = Main.createSenseiRequest();
       String strRepresentation = JsonSerializer.serialize(senseiRequest).toString();
       SenseiClientRequest senseiRequest2 = JsonDeserializer.deserialize(SenseiClientRequest.class, new JSONObject(strRepresentation));
       assertEquals(senseiRequest2.getFacets().size(), 1);
       System.out.println(senseiRequest2.toString());
       String strRepresentation2 = JsonSerializer.serialize(senseiRequest2).toString();
       System.out.println(strRepresentation2);
       assertEquals(strRepresentation2, strRepresentation);
       
    }
    @Test
    public void test3DeserializeFacetInit() throws Exception {       
        SenseiClientRequest senseiRequest =  SenseiClientRequest.builder()
                .addFacetInit("name", "parameter", FacetInit.build("string", "val1", "val2")).build();
       String strRepresentation = JsonSerializer.serialize(senseiRequest).toString();
       SenseiClientRequest senseiRequest2 = JsonDeserializer.deserialize(SenseiClientRequest.class, new JSONObject(strRepresentation));
       String strRepresentation2 = JsonSerializer.serialize(senseiRequest2).toString();
       System.out.println(strRepresentation2);
       assertEquals(strRepresentation2, strRepresentation);
       
    }
}
