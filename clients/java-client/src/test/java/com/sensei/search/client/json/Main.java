package com.sensei.search.client.json;

import java.util.Arrays;

import com.sensei.search.client.json.req.Facet;
import com.sensei.search.client.json.req.FacetInit;
import com.sensei.search.client.json.req.SelectionContainer;
import com.sensei.search.client.json.req.SenseiClientRequest;
import com.sensei.search.client.json.req.Sort;
import com.sensei.search.client.json.res.SenseiResult;

public class Main {
    public static void main(String[] args) throws Exception {
        SenseiClientRequest senseiRequest = createSenseiRequest();
        Object serialized = JsonSerializer.serialize(senseiRequest);
        System.out.println(serialized);        
        SenseiResult senseiResult = new SenseiServiceProxy().sendRequest("http://localhost:8079/sensei/", senseiRequest);
        System.out.println(senseiResult.toString());
    }

	public static SenseiClientRequest createSenseiRequest() {
		SenseiClientRequest senseiRequest = SenseiClientRequest.builder()
        .query("{\"query\" : \"this AND that OR thus\"}")
        .paging(5, 2)
        .groupBy(7, "car", "year")
        .addSelection(SelectionContainer.path("field", "value", true, 1))
        .addSelection(SelectionContainer.range("color", "*", "*"))
        .addFacet("facet1", Facet.builder().max(2).minCount(1).orderByVal().build())      
        .addFacetInit("name", "parameter", FacetInit.build("string", "val1", "val2"))
        .addSort(Sort.desc("color"))
        .addSort(Sort.asc("year"))
        .addTermVector("Term1")
        .explain(true)
        .partitions(Arrays.asList(1,2))        
        .build();
		return senseiRequest;
	}
}
