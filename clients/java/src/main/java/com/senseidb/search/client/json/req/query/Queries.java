package com.senseidb.search.client.json.req.query;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.senseidb.search.client.json.req.Operator;
import com.senseidb.search.client.json.req.Range;
import com.senseidb.search.client.json.req.Term;
import com.senseidb.search.client.json.req.Terms;
import com.senseidb.search.client.json.req.filter.Filter;
import com.senseidb.search.client.json.req.filter.Ids;
import com.senseidb.search.client.json.req.query.TextQuery.Type;
import com.senseidb.search.client.json.req.query.span.SpanTerm;

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
    public static QueryPrefix prefix(String field, String value, double boost) {
        return new QueryPrefix(field, value,  boost);
    }
    public static QueryWildcard wildcard(String field,String value, double boost) {
        return new QueryWildcard(field, value, boost);
    }
    public static BoolQuery bool(List<Query> must, List<Query> must_not, List<Query> should, int minimumNumberShouldMatch,
            double boost, boolean disableCoord) {
        return new BoolQuery(must, must_not, should, minimumNumberShouldMatch, boost, disableCoord);
    }
    public static BoolQuery bool(List<Query> must, List<Query> must_not, List<Query> should,
        double boost) {
    return new BoolQuery(must, must_not, should, null, boost, null);
}
    public static StringQuery.Builder stringQueryBuilder() {
        return StringQuery.builder();
    }
    public static StringQuery stringQuery(String query) {
      return StringQuery.builder().query(query).build();
  }
    public static com.senseidb.search.client.json.req.query.span.SpanFirst spanFirst(SpanTerm match, int end, double boost) {
        return new com.senseidb.search.client.json.req.query.span.SpanFirst(match,  end, boost);
    }
    public static com.senseidb.search.client.json.req.query.span.SpanNear spanNear(List<SpanTerm> clauses, int slop, boolean inOrder, boolean collectPayloads, double boost) {
        return new com.senseidb.search.client.json.req.query.span.SpanNear(clauses,  slop,  inOrder,  collectPayloads, boost);
    }
    public static com.senseidb.search.client.json.req.query.span.SpanNot spanNot(SpanTerm include, SpanTerm exclude, double boost) {
        return new com.senseidb.search.client.json.req.query.span.SpanNot( include,  exclude, boost);
    }
    public static com.senseidb.search.client.json.req.query.span.SpanOr spanOr(Double boost, SpanTerm... clauses) {
        return new com.senseidb.search.client.json.req.query.span.SpanOr(Arrays.asList(clauses), boost);
    }
    public static SpanTerm spanTerm(String field, String value) {
      return new SpanTerm(field,  value,  null);
  }
    public static SpanTerm spanTerm(String field, String value, Double boost) {
        return new SpanTerm(field,  value,  boost);
    }
    public static TextQuery textQuery(String field, String text, Operator operator, Type type, double boost) {
        return new TextQuery(field, text, operator, type, boost);
    }
    public static TextQuery textQuery(String field, String text, Operator operator, double boost) {
      return new TextQuery(field, text, operator, null, boost);
  }
    public static FilteredQuery filteredQuery(Query query, Filter filter, double boost) {
        return new FilteredQuery(query, filter, boost);
    }
    public static PathQuery path(String field, String name, double boost) {
        return new PathQuery(field, name, boost);
    }
    public static Term term(String field, String value, double boost) {
        return (Term)new Term(value, boost).setField(field);
    }
    public static Terms terms(String field, List<String> values, List<String> excludes, Operator op, int minimumMatch, double boost) {
        return (Terms) new Terms(values,excludes, op, minimumMatch, boost).setField(field);
    }
    public static Ids ids(List<String> values, List<String> excludes, double boost) {
        return new Ids(values, excludes, boost);
    }
    public static Range range(String field, String from, String to, boolean includeLower, boolean includeUpper, double boost, boolean noOptimize) {
      return (Range) new Range(from, to, includeLower, includeUpper, boost, noOptimize).setField(field);
  }
    public static Range range(String field, String from, String to, boolean includeLower, boolean includeUpper, double boost, boolean noOptimize, String type) {
      return (Range) new Range(from, to, includeLower, includeUpper, boost, noOptimize, type).setField(field);
  }
}
