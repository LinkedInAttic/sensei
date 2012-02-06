package com.senseidb.search.client.json.req.query;

import java.util.HashMap;
import java.util.Map;

import org.json.JSONException;
import org.json.JSONObject;

import com.senseidb.search.client.json.JsonHandler;
import com.senseidb.search.client.json.JsonSerializer;
import com.senseidb.search.client.json.req.Range;
import com.senseidb.search.client.json.req.Selection;
import com.senseidb.search.client.json.req.Term;
import com.senseidb.search.client.json.req.Terms;
import com.senseidb.search.client.json.req.filter.Ids;
import com.senseidb.search.client.json.req.query.span.SpanFirst;
import com.senseidb.search.client.json.req.query.span.SpanNear;
import com.senseidb.search.client.json.req.query.span.SpanNot;
import com.senseidb.search.client.json.req.query.span.SpanOr;
import com.senseidb.search.client.json.req.query.span.SpanTerm;

public class QueryJsonHandler implements JsonHandler<Query> {
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
    typeNames.put(BoolQuery.class, "bool");
    typeNames.put(Term.class, "term");
    typeNames.put(Terms.class, "terms");
    typeNames.put(Ids.class, "ids");
    typeNames.put(Range.class, "range");
  }

  @Override
  public JSONObject serialize(Query bean) throws JSONException {
    if (bean == null) {
      return null;
    }
    if (!typeNames.containsKey(bean.getClass())) {
      throw new UnsupportedOperationException("Class " + bean.getClass()
          + " is not supported for serialization by the QueryJsonHandler");
    }
    JSONObject defaultSerialization = (JSONObject) JsonSerializer.serialize(bean, false);
    if (bean instanceof FieldAware) {
      defaultSerialization.remove("field");
      if (bean instanceof SpanTerm && ((SpanTerm) bean).getBoost() == null) {
        SpanTerm spanTerm = (SpanTerm) bean;
        defaultSerialization = new JSONObject().put(spanTerm.getField(), spanTerm.getValue());
      } else {
        defaultSerialization = new JSONObject().put(((FieldAware) bean).getField(), defaultSerialization);
      }
    }
    if (bean instanceof Selection) {
      defaultSerialization.remove("field");
      defaultSerialization = new JSONObject().put(((Selection) bean).getField(), defaultSerialization);
    }
    return new JSONObject().put(typeNames.get(bean.getClass()), defaultSerialization);
  }

  @Override
  public Query deserialize(JSONObject json) throws JSONException {

    return null;
  }
}
