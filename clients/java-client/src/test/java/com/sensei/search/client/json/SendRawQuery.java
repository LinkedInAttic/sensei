package com.sensei.search.client.json;

import java.util.Arrays;
import java.util.Collections;

import com.sensei.search.client.json.req.Facet;
import com.sensei.search.client.json.req.FacetInit;
import com.sensei.search.client.json.req.Operator;
import com.sensei.search.client.json.req.Selection;
import com.sensei.search.client.json.req.SenseiClientRequest;
import com.sensei.search.client.json.req.Sort;
import com.sensei.search.client.json.res.SenseiResult;

public class SendRawQuery {
    public static void main(String[] args) throws Exception {
        //String request = new String(IOUtils.getBytes(SendRawQuery.class.getClassLoader().getResourceAsStream("json/car-query.json")), "UTF-8");
        SenseiClientRequest senseiRequest = SenseiClientRequest.builder()              
                .paging(10, 0)
                .fetchStored(true)
                .addSelection(Selection.terms("color", Arrays.asList("red", "blue"), Collections.EMPTY_LIST, Operator.or))
                     
                .build();
        String requestStr = JsonSerializer.serialize(senseiRequest).toString();
        System.out.println(requestStr);
        SenseiResult senseiResult = new SenseiServiceProxy().sendRequest("http://localhost:8080/sensei", senseiRequest);
        System.out.println(senseiResult.toString());
    }
}
