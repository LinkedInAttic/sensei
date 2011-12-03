package com.sensei.search.client.json.req.query;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.sensei.search.client.json.req.Operator;
import com.sensei.search.client.json.req.filter.Bool;
import com.sensei.search.client.json.req.filter.Filter;
import com.sensei.search.client.json.req.query.TextQuery.Type;
import com.sensei.search.client.json.req.query.span.SpanTerm;

public class Queries {
    public static CustomQuery customQuery(String cls, Map<String, String> params, double boost) {
        return new CustomQuery(cls, params,  boost);
    }
    public static DisMax disMax(double tieBraker, double boost, Query... queries) {
        return new DisMax( tieBraker,  boost,  Arrays.asList(queries));
    }
    public static MatchAllQuery matchAllQuery(double boost) {
        return new MatchAllQuery( boost);
    }
    public static QueryPrefix queryPrefix(String value, double boost) {
        return new QueryPrefix( value,  boost);
    }
    public static QueryWildcard queryWildcard(String value, double boost) {
        return new QueryWildcard( value,  boost);
    }
    public static Bool bool(List<Filter> must, List<Filter> must_not, List<Filter> should, Boolean minimumNumberShouldMatch,
            Double boost, Boolean disableCoord) {
        return new Bool(must, must_not, should, minimumNumberShouldMatch, boost, disableCoord);
    }
    public static StringQuery.Builder stringQueryBuilder() {
        return StringQuery.builder();
    }
    public static com.sensei.search.client.json.req.query.span.SpanFirst spanFirst(SpanTerm match, int end) {
        return new com.sensei.search.client.json.req.query.span.SpanFirst(match,  end);
    }
    public static com.sensei.search.client.json.req.query.span.SpanNear spanNear(List<SpanTerm> clauses, int slop, boolean inOrder, boolean collectPayloads) {
        return new com.sensei.search.client.json.req.query.span.SpanNear(clauses,  slop,  inOrder,  collectPayloads);
    }
    public static com.sensei.search.client.json.req.query.span.SpanNot spanNot(SpanTerm include, SpanTerm exclude) {
        return new com.sensei.search.client.json.req.query.span.SpanNot( include,  exclude);
    }
    public static com.sensei.search.client.json.req.query.span.SpanOr spanOr(SpanTerm... clauses) {
        return new com.sensei.search.client.json.req.query.span.SpanOr(Arrays.asList(clauses));
    }
    public static SpanTerm spanTerm(String value, double boost) {
        return new SpanTerm( value,  boost);
    }
    public static TextQuery textQuery(String message, Operator operator, Type type) {
        return new TextQuery(message, operator, type);
    }
    public static FilteredQuery filteredQuery(Query query, Filter filter) {
        return new FilteredQuery(query, filter);
    }
    public static PathQuery path(String name) {
        return new PathQuery(name);
    }    
}
