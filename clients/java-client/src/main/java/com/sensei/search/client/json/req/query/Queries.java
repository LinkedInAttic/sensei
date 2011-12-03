package com.sensei.search.client.json.req.query;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.sensei.search.client.json.req.Operator;
import com.sensei.search.client.json.req.Selection;
import com.sensei.search.client.json.req.Term;
import com.sensei.search.client.json.req.Terms;
import com.sensei.search.client.json.req.filter.Filter;
import com.sensei.search.client.json.req.filter.Ids;
import com.sensei.search.client.json.req.query.TextQuery.Type;
import com.sensei.search.client.json.req.query.span.SpanTerm;

public class Queries {
    public static CustomQuery customQuery(String cls, Map<String, String> params, double boost) {
        return new CustomQuery(cls, params,  boost);
    }
    public static DisMax disMax(double tieBraker, double boost ,  Term... queries) {
        return new DisMax( tieBraker, Arrays.asList(queries), boost);
    }
    public static MatchAllQuery matchAllQuery(double boost) {
        return new MatchAllQuery( boost);
    }
    public static QueryPrefix queryPrefix(String value, double boost) {
        return new QueryPrefix( value,  boost);
    }
    public static QueryWildcard queryWildcard(String value, double boost) {
        return new QueryWildcard( value, boost);
    }
    public static BoolQuery bool(List<Query> must, List<Query> must_not, List<Query> should, int minimumNumberShouldMatch,
            double boost, boolean disableCoord) {
        return new BoolQuery(must, must_not, should, minimumNumberShouldMatch, boost, disableCoord);
    }
    public static StringQuery.Builder stringQueryBuilder() {
        return StringQuery.builder();
    }
    public static com.sensei.search.client.json.req.query.span.SpanFirst spanFirst(SpanTerm match, int end, double boost) {
        return new com.sensei.search.client.json.req.query.span.SpanFirst(match,  end, boost);
    }
    public static com.sensei.search.client.json.req.query.span.SpanNear spanNear(List<SpanTerm> clauses, int slop, boolean inOrder, boolean collectPayloads, double boost) {
        return new com.sensei.search.client.json.req.query.span.SpanNear(clauses,  slop,  inOrder,  collectPayloads, boost);
    }
    public static com.sensei.search.client.json.req.query.span.SpanNot spanNot(SpanTerm include, SpanTerm exclude, double boost) {
        return new com.sensei.search.client.json.req.query.span.SpanNot( include,  exclude, boost);
    }
    public static com.sensei.search.client.json.req.query.span.SpanOr spanOr(double boost, SpanTerm... clauses) {
        return new com.sensei.search.client.json.req.query.span.SpanOr(Arrays.asList(clauses), boost);
    }
    public static SpanTerm spanTerm(String value, double boost) {
        return new SpanTerm( value,  boost);
    }
    public static TextQuery textQuery(String message, Operator operator, Type type, double boost) {
        return new TextQuery(message, operator, type, boost);
    }
    public static FilteredQuery filteredQuery(Query query, Filter filter, double boost) {
        return new FilteredQuery(query, filter, boost);
    }
    public static PathQuery path(String name, double boost) {
        return new PathQuery(name, boost);
    }
    public static Term term(String field, String value, double boost) {
        return (Term)new Term(value, boost).setField(field);
    }
    public static Selection terms(String field, List<String> values, List<String> excludes, Operator op, double boost) {
        return new Terms(values,excludes, op, boost).setField(field);
    }
    public static Ids ids(List<String> values, List<String> excludes, double boost) {
        return new Ids(values, excludes, boost);
    }
}
