package com.sensei.search.client.json.req.filter;

import java.util.Arrays;

import com.sensei.search.client.json.CustomJsonHandler;
import com.sensei.search.client.json.req.Operator;
import com.sensei.search.client.json.req.Selection;
import com.sensei.search.client.json.req.SelectionJsonHandler;
import com.sensei.search.client.json.req.query.FilteredQuery;
import com.sensei.search.client.json.req.query.Query;
import com.sensei.search.client.json.req.query.QueryJsonHandler;


public class FilterRoot {
    @CustomJsonHandler(value = FilterJsonHandler.class, flatten=true)
    private Ids ids;
    @CustomJsonHandler(value = FilterJsonHandler.class, flatten=true)
    private Filter.AndOr and;
    @CustomJsonHandler(value = FilterJsonHandler.class, flatten=true)
    private Filter.AndOr or;
    @CustomJsonHandler(value = QueryJsonHandler.class, flatten=true)
    private Query query;
    @CustomJsonHandler(value = SelectionJsonHandler.class, flatten=true)
    private Selection.Term term;
    @CustomJsonHandler(value = SelectionJsonHandler.class, flatten=true)
    private Selection.Terms terms;
    @CustomJsonHandler(value = SelectionJsonHandler.class, flatten=true)
    private Selection.Path path;
    @CustomJsonHandler(value = SelectionJsonHandler.class, flatten=true)
    private Selection.Range range;
    @CustomJsonHandler(value = FilterJsonHandler.class, flatten=true)
    private Bool bool;
    
    public static class Builder {
        FilterRoot rootFilter = new FilterRoot();
        public Builder ids(Ids ids) {
            if (rootFilter.ids != null) {
                throw new IllegalStateException("The attribute was already set");
            }
            rootFilter.ids = ids;
            return this;
        }
        public Builder and(Filter... filters) {
            if (rootFilter.and != null) {
                throw new IllegalStateException("The attribute was already set");
            }
            rootFilter.and = new Filter.AndOr(Arrays.asList(filters), Operator.and);
            return this;
        }
        public Builder or(Filter... filters) {
            if (rootFilter.or != null) {
                throw new IllegalStateException("The attribute was already set");
            }
            rootFilter.or = new Filter.AndOr(Arrays.asList(filters), Operator.or);
            return this;
        }
        public Builder query(Query query) {
            if (rootFilter.query != null) {
                throw new IllegalStateException("The attribute was already set");
            }
            rootFilter.query = query;
            return this;
        }
       
        public Builder bool(Bool bool) {
            if (rootFilter.bool != null) {
                throw new IllegalStateException("The attribute was already set");
            }
            rootFilter.bool = bool;
            return this;
        }
        public Builder term(Selection.Term term) {
            if (rootFilter.term != null) {
                throw new IllegalStateException("The attribute was already set");
            }
            rootFilter.term = term;
            return this;
        }
        public Builder terms(Selection.Terms terms) {
            if (rootFilter.terms != null) {
                throw new IllegalStateException("The attribute was already set");
            }
            rootFilter.terms = terms;
            return this;
        }
        public Builder path(Selection.Path path) {
            if (rootFilter.path != null) {
                throw new IllegalStateException("The attribute was already set");
            }
            rootFilter.path = path;
            return this;
        }
        public Builder range(Selection.Range range) {
            if (rootFilter.range != null) {
                throw new IllegalStateException("The attribute was already set");
            }
            rootFilter.range = range;
            return this;
        }
       public FilterRoot build() {
           return rootFilter;
       } 
    }
    public static Builder builder() {
        return new Builder();
    }
}
