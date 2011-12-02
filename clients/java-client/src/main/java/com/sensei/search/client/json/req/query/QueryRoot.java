package com.sensei.search.client.json.req.query;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.sensei.search.client.json.CustomJsonHandler;
import com.sensei.search.client.json.JsonField;
import com.sensei.search.client.json.req.Selection;
import com.sensei.search.client.json.req.SelectionJsonHandler;
import com.sensei.search.client.json.req.filter.Bool;
import com.sensei.search.client.json.req.filter.FilterJsonHandler;
import com.sensei.search.client.json.req.query.span.SpanFirst;
import com.sensei.search.client.json.req.query.span.SpanNear;
import com.sensei.search.client.json.req.query.span.SpanNot;
import com.sensei.search.client.json.req.query.span.SpanOr;
import com.sensei.search.client.json.req.query.span.SpanTerm;

public class QueryRoot {
    @JsonField("match_all")
    @CustomJsonHandler(value = QueryJsonHandler.class, flatten = true)
    private MatchAllQuery matchAllQuery;
    @JsonField("query_string")
    @CustomJsonHandler(value = QueryJsonHandler.class, flatten = true)
    private StringQuery queryString;
    @JsonField("dis_max")
    @CustomJsonHandler(value = QueryJsonHandler.class, flatten = true)
    private DisMax disMax;
    @CustomJsonHandler(value = QueryJsonHandler.class, flatten = true)
    private TextQuery text;
    @CustomJsonHandler(value = SelectionJsonHandler.class, flatten = true)
    private Selection.Term term;
    @CustomJsonHandler(value = FilterJsonHandler.class, flatten = true)
    private Bool bool;
    @CustomJsonHandler(value = QueryJsonHandler.class, flatten = true)
    private QueryPrefix prefix;
    @CustomJsonHandler(value = QueryJsonHandler.class, flatten = true)
    private QueryWildcard wildcard;
    @CustomJsonHandler(value = SelectionJsonHandler.class, flatten = true)
    private Selection.Range range;
    @CustomJsonHandler(value = SelectionJsonHandler.class, flatten = true)
    private Selection.Terms terms;
    @JsonField("span_term")
    @CustomJsonHandler(value = QueryJsonHandler.class, flatten = true)
    private SpanTerm spanTerm;
    @JsonField("span_near")
    @CustomJsonHandler(value = QueryJsonHandler.class, flatten = true)
    private SpanNear spanNear;
    @JsonField("span_or")
    @CustomJsonHandler(value = QueryJsonHandler.class, flatten = true)
    private SpanOr spanOr;
    @JsonField("span_not")
    @CustomJsonHandler(value = QueryJsonHandler.class, flatten = true)
    private SpanNot spanNot;
    @JsonField("span_first")
    @CustomJsonHandler(value = QueryJsonHandler.class, flatten = true)
    private SpanFirst spanFirst;
    @CustomJsonHandler(value = QueryJsonHandler.class, flatten = true)
    private CustomQuery custom;
    @CustomJsonHandler(value = QueryJsonHandler.class, flatten = true)
    private FilteredQuery filtered;
    @CustomJsonHandler(value = QueryJsonHandler.class, flatten = true)
    private PathQuery path;
    public static class Builder {
        private QueryRoot rootQuery = new QueryRoot();
        public Builder customQuery(CustomQuery customQuery) {
            rootQuery.custom = customQuery;
            return this;
        }
        public Builder disMax(DisMax disMax) {
            rootQuery.disMax = disMax;
            return this;
        }
        public Builder matchAllQuery(MatchAllQuery matchAllQuery) {
            rootQuery.matchAllQuery = matchAllQuery;
            return this;
        }
        public Builder queryPrefix(QueryPrefix queryPrefix) {
            rootQuery.prefix = queryPrefix;
            return this;
        }
        public Builder queryWildcard(QueryWildcard queryWildcard) {
            rootQuery.wildcard = queryWildcard;
            return this;
        }
        public Builder stringQuery(StringQuery stringQuery) {
            rootQuery.queryString = stringQuery;
            return this;
        }
        public Builder spanFirst(com.sensei.search.client.json.req.query.span.SpanFirst spanFirst ) {
            rootQuery.spanFirst = spanFirst;
            return this;
        }
        public Builder spanNear(SpanNear spanNear) {
            rootQuery.spanNear = spanNear;
            return this;
        }
        public Builder spanNot(SpanNot spanNot) {
            rootQuery.spanNot = spanNot;
            return this;
        }
        public Builder spanOr(SpanOr spanOr) {
            rootQuery.spanOr = spanOr;
            return this;
        }
        public Builder spanTerm(SpanTerm spanTerm) {
            rootQuery.spanTerm = spanTerm;
            return this;
        }
        public Builder filteredQuery(FilteredQuery filtered) {
            rootQuery.filtered = filtered;
            return this;
        }
        public Builder textQuery(TextQuery textQuery) {
            rootQuery.text = textQuery;
            return this;
        }
        public Builder path(PathQuery pathQuery) {
            rootQuery.path = pathQuery;
            return this;
        }
        public QueryRoot build() {
            return rootQuery;
        }
    }
    public static Builder builder() {
        return new Builder();
    }
}
