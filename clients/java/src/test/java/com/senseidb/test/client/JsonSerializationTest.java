/**
 * This software is licensed to you under the Apache License, Version 2.0 (the
 * "Apache License").
 *
 * LinkedIn's contributions are made under the Apache License. If you contribute
 * to the Software, the contributions will be deemed to have been made under the
 * Apache License, unless you expressly indicate otherwise. Please do not make any
 * contributions that would be inconsistent with the Apache License.
 *
 * You may obtain a copy of the Apache License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, this software
 * distributed under the Apache License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the Apache
 * License for the specific language governing permissions and limitations for the
 * software governed under the Apache License.
 *
 * © 2012 LinkedIn Corp. All Rights Reserved.  
 */

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
       System.out.println("Running test2Serialization...");
        SenseiClientRequest senseiRequest = Examples.basicWithSelections(SenseiClientRequest.builder()).build();
       String strRepresentation = JsonSerializer.serialize(senseiRequest).toString();
       System.out.println("strRepresentation: " + strRepresentation);
       SenseiClientRequest senseiRequest2 = JsonDeserializer.deserialize(SenseiClientRequest.class, new JSONObject(strRepresentation));
       assertEquals(senseiRequest2.getFacets().size(), 1);
       System.out.println("senseiRequest2: " + senseiRequest2.toString());
       String strRepresentation2 = JsonSerializer.serialize(senseiRequest2).toString();
       System.out.println("strRepresentation2: " + strRepresentation2);
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
