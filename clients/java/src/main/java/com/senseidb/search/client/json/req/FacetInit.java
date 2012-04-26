package com.senseidb.search.client.json.req;

import java.util.Arrays;
import java.util.List;

public class FacetInit {
    String type; List<Object> values;
    
    public static FacetInit build(FacetType type, Object... values) {
        FacetInit facetInit = new FacetInit();
        facetInit.type = type.getValue();
        facetInit.values = Arrays.asList(values);
        return facetInit;
    }
    public static FacetInit build(FacetType type, List<Object> values) {
        FacetInit facetInit = new FacetInit();
        facetInit.type = type.getValue();
        facetInit.values = values;
        return facetInit;
    }
}
