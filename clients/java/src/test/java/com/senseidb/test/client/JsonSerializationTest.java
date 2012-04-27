package com.senseidb.test.client;


import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import com.senseidb.search.client.json.JsonDeserializer;
import com.senseidb.search.client.json.JsonSerializer;
import com.senseidb.search.client.req.FacetInit;
import com.senseidb.search.client.req.FacetType;
import com.senseidb.search.client.req.SenseiClientRequest;
import com.senseidb.search.client.res.SenseiResult;

public class JsonSerializationTest extends Assert {

    @Test
    public void test1Deserialization() throws Exception {
        String response = new String(IOUtils.getBytes(getClass().getClassLoader().getResourceAsStream("json/senseiresult.json")), "UTF-8");
        System.out.println(new JSONObject(response).toString(2));
        SenseiResult senseiResult = JsonDeserializer.deserialize(SenseiResult.class, new JSONObject(response));
        assertEquals(senseiResult.getFacets().size(), 2);
        System.out.println(senseiResult);
    }
    @Test
    public void test2Serialization() throws Exception {
        SenseiClientRequest senseiRequest = Examples.basicWithSelections(SenseiClientRequest.builder()).build();
       String strRepresentation = JsonSerializer.serialize(senseiRequest).toString();
       System.out.println(strRepresentation);
       SenseiClientRequest senseiRequest2 = JsonDeserializer.deserialize(SenseiClientRequest.class, new JSONObject(strRepresentation));
       assertEquals(senseiRequest2.getFacets().size(), 1);
       System.out.println(senseiRequest2.toString());
       String strRepresentation2 = JsonSerializer.serialize(senseiRequest2).toString();
       System.out.println(strRepresentation2);
       assertEquals(strRepresentation2, strRepresentation);

    }
    //@Test
    public void test3DeserializeFacetInit() throws Exception {
        SenseiClientRequest senseiRequest =  SenseiClientRequest.builder()
                .addFacetInit("name", "parameter", FacetInit.build(FacetType.type_float, "val1", "val2")).build();
       String strRepresentation = JsonSerializer.serialize(senseiRequest).toString();
       SenseiClientRequest senseiRequest2 = JsonDeserializer.deserialize(SenseiClientRequest.class, new JSONObject(strRepresentation));
       String strRepresentation2 = JsonSerializer.serialize(senseiRequest2).toString();
       System.out.println(strRepresentation2);
       assertEquals(strRepresentation2, strRepresentation);

    }
    //@Test
    public void test4FiltersSerialization() throws Exception {
        SenseiClientRequest senseiRequest =  Examples.filters(SenseiClientRequest.builder()).build();
       JSONObject json = (JSONObject) JsonSerializer.serialize(senseiRequest);
       System.out.println(json.toString(3));
    }
    //@Test
    public void test5QueriesSerialization() throws Exception {
        SenseiClientRequest senseiRequest =  Examples.queries(SenseiClientRequest.builder()).build();
       JSONObject json = (JSONObject) JsonSerializer.serialize(senseiRequest);
       System.out.println(json.toString(3));
    }
}
