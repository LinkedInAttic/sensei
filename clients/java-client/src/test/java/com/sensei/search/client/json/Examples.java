package com.sensei.search.client.json;

import java.util.Arrays;

import com.sensei.search.client.json.req.Facet;
import com.sensei.search.client.json.req.FacetInit;
import com.sensei.search.client.json.req.Operator;
import com.sensei.search.client.json.req.Selection;
import com.sensei.search.client.json.req.SenseiClientRequest;
import com.sensei.search.client.json.req.Sort;
import com.sensei.search.client.json.req.filter.FilterRoot;
import com.sensei.search.client.json.req.filter.Filters;
import com.sensei.search.client.json.req.query.Queries;
import com.sensei.search.client.json.req.query.QueryRoot;
import com.sensei.search.client.json.req.query.TextQuery.Type;
import com.sensei.search.client.json.res.SenseiResult;

public class Examples {
    public static void main(String[] args) throws Exception {
        System.out.println(Selection.class.isAnnotationPresent(CustomJsonHandler.class));
        System.out.println(Arrays.asList(Selection.Path.class.getDeclaredAnnotations()));
        System.out.println(Selection.Path.class.isAnnotationPresent(CustomJsonHandler.class));
        SenseiClientRequest senseiRequest = basicWithSelections(SenseiClientRequest.builder()).build();
        Object serialized = JsonSerializer.serialize(senseiRequest);
        System.out.println(serialized);        
        SenseiResult senseiResult = new SenseiServiceProxy().sendRequest("http://localhost:8079/sensei/", senseiRequest);
        System.out.println(senseiResult.toString());
    }
    public static SenseiClientRequest.Builder basicWithSelections(SenseiClientRequest.Builder builder) {
        builder.paging(5, 2)
        .groupBy(7, "car", "year")
        .addSelection(Selection.path("field", "value", true, 1))
        .addSelection(Selection.range("color", "*", "*"))
        .addFacet("facet1", Facet.builder().max(2).minCount(1).orderByVal().build())      
        .addFacetInit("name", "parameter", FacetInit.build("string", "val1", "val2"))
        .addSort(Sort.desc("color"))
        .addSort(Sort.asc("year"))
        .addTermVector("Term1")
        .explain(true)
        .partitions(Arrays.asList(1,2)) ;
        return builder;
    }
    public static SenseiClientRequest.Builder filters(SenseiClientRequest.Builder builder) {
        builder.filterRoot(FilterRoot.builder()
                .query(Queries.textQuery("this AND that OR thus", Operator.or, Type.phrase_prefix))
                .and(
                    Filters.boolMust(Filters.or(Filters.term("field", "value"))),
                    Queries.textQuery("this AND that OR thus", Operator.or, Type.phrase_prefix), 
                    Filters.and(
                                Filters.or(Filters.term("field", "value"), 
                                           Filters.terms("Field", Arrays.asList("a","b"), Arrays.asList("a","b"), Operator.or)
                                )
                    ),
                    Filters.ids(Arrays.asList("val2", "val3"), Arrays.asList("ExcludedValue")),
                    Filters.range("field1", "*", "*"))
                .or(Filters.ids(Arrays.asList("val2", "val3"), Arrays.asList("ExcludedValue")), Filters.range("field1", "*", "*", true, true))    
                .ids(Filters.ids(Arrays.asList("val2", "val3"), Arrays.asList("ExcludedValue")))
                .term(Filters.term("field", "value"))
                 .bool(Filters.boolShould(Filters.or(Filters.term("field", "value"))))
                    .build());  
        return builder;
    }
	
	public static SenseiClientRequest.Builder queries(SenseiClientRequest.Builder builder) {
       
       
	    builder.queryRoot(QueryRoot.builder()
            .disMax(Queries.disMax(2.0, 1.0, Queries.matchAllQuery(3)))
            .filteredQuery(Queries.filteredQuery(Queries.disMax(2.0, 1.0, Queries.matchAllQuery(3)), Filters.ids(Arrays.asList("val2", "val3"), Arrays.asList("ExcludedValue"))))
            .matchAllQuery(Queries.matchAllQuery(3.0))
            .path(Queries.path("/path1/path2"))
            .queryPrefix(Queries.queryPrefix("prefix", 2.0))
            .queryWildcard(Queries.queryWildcard("wildcard", 3.0))
            .spanFirst(Queries.spanFirst(Queries.spanTerm("val", 3.5), 3)).
            spanNear(Queries.spanNear(Arrays.asList(Queries.spanTerm("val", 3.5)), 3, true, true))
            .spanNot(Queries.spanNot(Queries.spanTerm("val", 3.5), Queries.spanTerm("val2", 3.5)))
            .spanOr(Queries.spanOr(Queries.spanTerm("val", 3.5)))
            .spanTerm(Queries.spanTerm("val", 3.5))
            .textQuery(Queries.textQuery("text", Operator.or, Type.phrase))
            .stringQuery(
                    Queries.stringQueryBuilder().autoGeneratePhraseQueries(true).defaultField("field").defaultOperator(Operator.and).fields("field1","field2").tieBreaker(2).build()
            ).build());
             
                   
        
        return builder;
    }
	
	public static SenseiClientRequest createFullSenseiRequestWithOnlyFilters() {
        SenseiClientRequest senseiRequest = SenseiClientRequest.builder()
        
        .paging(5, 2)
        .groupBy(7, "car", "year")
        .addSelection(Selection.path("field", "value", true, 1))
        .addSelection(Selection.range("color", "*", "*"))
        .addFacet("facet1", Facet.builder().max(2).minCount(1).orderByVal().build())      
        .addFacetInit("name", "parameter", FacetInit.build("string", "val1", "val2"))
        .addSort(Sort.desc("color"))
        .addSort(Sort.asc("year"))
        .addTermVector("Term1")
        .explain(true)
        .partitions(Arrays.asList(1,2))        
        .filterRoot(FilterRoot.builder().
                and(
                    Filters.boolMust(Filters.or(Filters.term("field", "value"))),
                    Queries.disMax(2.0, 4.3, Queries.queryPrefix("prefix", 1.0)), 
                    Filters.and(
                                Filters.or(Filters.term("field", "value"), 
                                           Filters.terms("Field", Arrays.asList("a","b"), Arrays.asList("a","b"), Operator.or)
                                )
                    ),
                    Filters.ids(Arrays.asList("val2", "val3"), Arrays.asList("ExcludedValue")),
                    Filters.range("field1", "*", "*"))
                .or(Filters.ids(Arrays.asList("val2", "val3"), Arrays.asList("ExcludedValue")), Filters.range("field1", "*", "*"))    
                .ids(Filters.ids(Arrays.asList("val2", "val3"), Arrays.asList("ExcludedValue")))
                .term(Filters.term("field", "value"))
                 .bool(Filters.boolShould(Filters.or(Filters.term("field", "value"))))
                    .build())          
        .build(); 
        return senseiRequest;
    }
}
