package com.sensei.search.client.json.req.filter;

import java.util.Arrays;
import java.util.List;

import org.json.JSONException;
import org.json.JSONObject;

import com.sensei.search.client.json.req.Operator;
import com.sensei.search.client.json.req.Selection;
import com.sensei.search.client.json.req.Selection.Path;
import com.sensei.search.client.json.req.Selection.Range;
import com.sensei.search.client.json.req.Selection.Term;
import com.sensei.search.client.json.req.Selection.Terms;
import com.sensei.search.client.json.req.filter.Filter.AndOr;
import com.sensei.search.client.json.req.filter.FilterRoot.Builder;
import com.sensei.search.client.json.req.query.Query;
import com.sensei.search.client.json.req.query.StringQuery;

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
   
    public static Bool bool(List<Filter> must, List<Filter> must_not, List<Filter> should) {
        return new Bool(must, must_not, should);
    }
    public static Bool boolMust(Filter... must) {
        return new Bool(Arrays.asList(must), null, null);
    }
    public static Bool boolMustNot(Filter... mustNot) {
        return new Bool( null, Arrays.asList(mustNot), null);
    }
    public static Bool boolShould(Filter... should) {
        return new Bool( null, null, Arrays.asList(should));
    }
    public static Selection.Term term(String field, String value) {
        return (Selection.Term)new Term(value).setField(field);       
    }
    public static Selection terms(String field, List<String> values, List<String> excludes, Operator op) {
        return (Selection.Terms)new Terms(values,excludes, op).setField(field);
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
