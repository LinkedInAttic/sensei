package com.sensei.search.res.json.domain;

public class FacetResult {
    private String value;
    private Boolean selected  = false;
    private Integer count;
    @Override
    public String toString() {
        return "FacetResult [value=" + value + ", selected=" + selected + ", count=" + count + "]";
    }
    
}
