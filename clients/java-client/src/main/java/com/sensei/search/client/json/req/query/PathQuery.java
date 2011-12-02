package com.sensei.search.client.json.req.query;

import com.sensei.search.client.json.CustomJsonHandler;

@CustomJsonHandler(value = QueryJsonHandler.class)
public class PathQuery implements Query {
    private String value;

    public PathQuery(String value) {
        super();
        this.value = value;
    }
    
}
