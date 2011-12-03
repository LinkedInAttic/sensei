package com.sensei.search.client.json.req.filter;

import java.util.Arrays;
import java.util.List;

import com.sensei.search.client.json.req.Operator;
import com.sensei.search.client.json.req.Selection;
import com.sensei.search.client.json.req.Selection.Path;
import com.sensei.search.client.json.req.Selection.Range;
import com.sensei.search.client.json.req.Term;
import com.sensei.search.client.json.req.Terms;
import com.sensei.search.client.json.req.filter.Filter.AndOr;
import com.sensei.search.client.json.req.query.Query;

public class Filters {
    public static Ids ids(List<String> values, List<String> excludes) {
        return new Ids(values, excludes);
    }
    public static AndOr and(Filter... filters) {
        return new AndOr(Arrays.asList(filters), Operator.and);
    }
    public static AndOr or(Filter... filters) {
        return new AndOr(Arrays.asList(filters), Operator.or);
    }
   
    public static Query query(Query query) {
        return query;
        
    }
   
    public static BoolFilter bool(List<Filter> must, List<Filter> must_not, List<Filter> should) {
        return new BoolFilter(must, must_not, should);
    }
    public static BoolFilter boolMust(Filter... must) {
        return new BoolFilter(Arrays.asList(must), null, null);
    }
    public static BoolFilter boolMustNot(Filter... mustNot) {
        return new BoolFilter( null, Arrays.asList(mustNot), null);
    }
    public static BoolFilter boolShould(Filter... should) {
        return new BoolFilter( null, null, Arrays.asList(should));
    }
    public static Term term(String field, String value) {
        return (Term)new Term(value).setField(field);       
    }
    public static Selection terms(String field, List<String> values, List<String> excludes, Operator op) {
        return (Terms)new Terms(values,excludes, op).setField(field);
    }
    public static Selection range(String field, String upper, String lower,boolean includeUpper, boolean includeLower) {
         return (Selection.Range) new Range(upper, lower, includeUpper, includeLower).setField(field);
    }
    public static Selection range(String field, String upper, String lower) {
        return (Selection.Range) new Range(upper, lower, true, true).setField(field);
   }
    public static Selection path(String field, String value, boolean strict, int depth) {
        return (Selection.Path) new Path(value, strict, depth).setField(field);
    }
}
