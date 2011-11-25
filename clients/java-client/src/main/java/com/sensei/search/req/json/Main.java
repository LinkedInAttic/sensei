package com.sensei.search.req.json;

import java.util.Arrays;

import com.sensei.search.req.json.domain.Facet;
import com.sensei.search.req.json.domain.FacetInit;
import com.sensei.search.req.json.domain.Selection;
import com.sensei.search.req.json.domain.SenseiRequest;
import com.sensei.search.res.json.domain.SenseiResult;

public class Main {
    public static void main(String[] args) throws Exception {
        SenseiRequest senseiRequest = SenseiRequest.builder()
        .query("{\"query\" : \"this AND that OR thus\"}")
        .count(2).from(3)
        .groupBy("car", "year").size(7)
        .addSelection("tags", 
                Selection.builder().values("val1", "val2")
                .excludes("excl1", "excl2")
                .operatorAnd().build())
        .addFacet("facet1", Facet.builder().max(2).minCount(1).orderByVal().build())
        //.addFacetInit("facetInit1", "param", FacetInit.build("string", "val1", "val2"))
       // .sortByDesc("column1").sortByRelevance()
        .explain(true)
        .partitions(Arrays.asList(1,2))        
        .build();
        Object serialized = JsonSerializer.serialize(senseiRequest);
        System.out.println(serialized);        
        SenseiResult senseiResult = new SenseiServiceProxy().sendRequest("http://localhost:8079/sensei/", senseiRequest);
        System.out.println(senseiResult.toString());
    }
}
