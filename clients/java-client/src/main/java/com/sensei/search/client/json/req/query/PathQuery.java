package com.sensei.search.client.json.req.query;

import com.sensei.search.client.json.CustomJsonHandler;

@CustomJsonHandler(value = QueryJsonHandler.class)
public class PathQuery extends FieldAware implements Query {
    private String value;
    private double boost;
    public PathQuery(String field, String value, double boost) {
        super();
        this.value = value;
        this.boost = boost;
        this.field = field;
    }

}
