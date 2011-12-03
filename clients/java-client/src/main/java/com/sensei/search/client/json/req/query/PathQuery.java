package com.sensei.search.client.json.req.query;

import com.sensei.search.client.json.CustomJsonHandler;

@CustomJsonHandler(value = QueryJsonHandler.class)
public class PathQuery implements Query {
    private String value;
    private double boost;
    public PathQuery(String value, double boost) {
        super();
        this.value = value;
        this.boost = boost;
    }

}
