package com.sensei.search.req.json.domain;

import java.util.Arrays;
import java.util.List;

public class FacetInit {
    String type; List<Object> values;
    public static FacetInit build(String type, Object... values) {
        FacetInit facetInit = new FacetInit();
        facetInit.type = type;
        facetInit.values = Arrays.asList(values);
        return facetInit;
    }
    public static FacetInit build(String type, List<Object> values) {
        FacetInit facetInit = new FacetInit();
        facetInit.type = type;
        facetInit.values = values;
        return facetInit;
    }
}
