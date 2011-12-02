package com.sensei.search.client.json.req.query;

import java.util.HashMap;
import java.util.Map;

import org.json.JSONException;
import org.json.JSONObject;

import com.sensei.search.client.json.JsonHandler;
import com.sensei.search.client.json.JsonSerializer;
import com.sensei.search.client.json.req.query.span.SpanFirst;
import com.sensei.search.client.json.req.query.span.SpanNear;
import com.sensei.search.client.json.req.query.span.SpanNot;
import com.sensei.search.client.json.req.query.span.SpanOr;
import com.sensei.search.client.json.req.query.span.SpanTerm;

public class QueryJsonHandler implements JsonHandler<Query>{
    private static Map<Class<? extends Query>, String> typeNames = new HashMap<Class<? extends Query>, String>();
    static {
        typeNames.put(StringQuery.class, "query_string");
        typeNames.put(MatchAllQuery.class, "match_all");
        typeNames.put(DisMax.class, "dis_max");
        typeNames.put(QueryPrefix.class, "prefix");
        typeNames.put(QueryWildcard.class, "wildcard");
        typeNames.put(TextQuery.class, "text");
        typeNames.put(SpanFirst.class, "span_first");
        typeNames.put(SpanTerm.class, "span_term");
        typeNames.put(SpanNear.class, "span_near");
        typeNames.put(SpanNot.class, "span_not");
        typeNames.put(SpanOr.class, "span_or"); 
        typeNames.put(CustomQuery.class, "custom"); 
        typeNames.put(TextQuery.class, "text"); 
        typeNames.put(FilteredQuery.class, "filtered"); 
        typeNames.put(PathQuery.class, "path");
    }
    @Override
    public JSONObject serialize(Query bean) throws JSONException {
        if (bean == null) {
            return null;
        }
        if (!typeNames.containsKey(bean.getClass())) {
            throw new UnsupportedOperationException("Class " + bean.getClass() + " is not supported for serialization by the QueryJsonHandler");
        }
        
        return new JSONObject().put(typeNames.get(bean.getClass()), JsonSerializer.serialize(bean, false));
    }
    @Override
    public Query deserialize(JSONObject json) throws JSONException {
       
        return null;
    }
}
