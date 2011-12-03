package com.sensei.search.client.json.req.query;

import java.util.List;

import com.sensei.search.client.json.CustomJsonHandler;
import com.sensei.search.client.json.JsonField;
@CustomJsonHandler(QueryJsonHandler.class)
public class BoolQuery implements Query {
    List<Query> must;
    List<Query> must_not;
    List<Query> should;
    @JsonField("minimum_number_should_match")
    int minimumNumberShouldMatch;
    double boost;
    boolean disableCoord;


    public BoolQuery(List<Query> must, List<Query> must_not, List<Query> should, int minimumNumberShouldMatch,
            double boost, boolean disableCoord) {
        super();
        this.must = must;
        this.must_not = must_not;
        this.should = should;
        this.minimumNumberShouldMatch = minimumNumberShouldMatch;
        this.boost = boost;
        this.disableCoord = disableCoord;
    }


}
