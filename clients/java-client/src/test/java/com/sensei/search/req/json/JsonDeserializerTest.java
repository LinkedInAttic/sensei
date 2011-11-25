package com.sensei.search.req.json;


import junit.framework.TestCase;

import org.json.JSONObject;
import org.junit.Test;

import com.sensei.search.res.json.domain.SenseiResult;

public class JsonDeserializerTest extends TestCase {
    
    @Test
    public void testDeserialization() throws Exception {
        String response = new String(IOUtils.getBytes(getClass().getClassLoader().getResourceAsStream("json/senseiresult.json")), "UTF-8");
        SenseiResult senseiResult = JsonDeserializer.deserialize(SenseiResult.class, new JSONObject(response));
        assertEquals(senseiResult.getFacets().size(), 1);
        System.out.println(senseiResult);
    }
}
