package com.senseidb.search.client.json.req.filter;

import java.util.ArrayList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.senseidb.search.client.json.JsonHandler;
import com.senseidb.search.client.json.JsonSerializer;
import com.senseidb.search.client.json.req.Selection;
import com.senseidb.search.client.json.req.SelectionJsonHandler;
import com.senseidb.search.client.json.req.filter.Filter.AndOr;
import com.senseidb.search.client.json.req.query.Query;
import com.senseidb.search.client.json.req.query.QueryJsonHandler;
import com.senseidb.search.client.json.req.query.StringQuery;

public class FilterJsonHandler implements JsonHandler<Filter>{
    private SelectionJsonHandler selectionJsonHandler = new SelectionJsonHandler();
    private QueryJsonHandler queryJsonHandler = new QueryJsonHandler();
    @Override
    public JSONObject serialize(Filter bean) throws JSONException {
        if (bean == null) {
            return null;
        }
        if (bean instanceof Selection) {
            return selectionJsonHandler.serialize((Selection) bean);
        }
        if (bean instanceof StringQuery) {
            JSONObject ret = (JSONObject) JsonSerializer.serialize(bean);
            return new JSONObject().put("query", ret);
        }
        if (bean instanceof AndOr) {
            AndOr andOr = (AndOr) bean;
            String operation = andOr.getOperation().name();

            List<JSONObject> filters = convertToJson(andOr.filters);
            return new JSONObject().put(operation, new JSONArray(filters));
        }
        if (bean instanceof BoolFilter) {
            BoolFilter bool = (BoolFilter) bean;
            JSONObject ret = new JSONObject();
            if (bool.getMust() != null) {
                ret.put("must", new JSONArray(convertToJson(bool.getMust())));
            }
            if (bool.getMust_not() != null) {
                ret.put("must_not", new JSONArray(convertToJson(bool.getMust_not())));
            }
            if (bool.getShould() != null) {
                ret.put("should", new JSONArray(convertToJson(bool.getShould())));
            }
            return new JSONObject().put("bool", ret);
        }
        if (bean instanceof Ids) {
            Ids ids = (Ids) bean;
            JSONObject ret = new JSONObject();
            if (ids.getValues() != null) {
                ret.put("values", new JSONArray(ids.getValues()));
            }
            if (ids.getExcludes() != null) {
                ret.put("excludes", new JSONArray(ids.getExcludes() ));
            }

            return new JSONObject().put("ids", ret);
        }
        if (bean instanceof IsNull) {
          IsNull isNull = (IsNull) bean;
          

          return new JSONObject().put("isNull", new JSONObject().put("field", isNull.getField()));
      }
        if (bean instanceof Query) {
            return queryJsonHandler.serialize((Query) bean);
        }
        if (bean instanceof QueryFilter) {
          return new JSONObject().put("query", queryJsonHandler.serialize(((QueryFilter) bean).getQuery()));
        }
        throw new UnsupportedOperationException(bean.getClass() + " is not supported");

    }

    private List<JSONObject> convertToJson(List<Filter> filters2) throws JSONException {
        List<JSONObject> filters = new ArrayList<JSONObject>(filters2.size());
        for(Filter filter :  filters2) {
            filters.add(serialize(filter));
        }
        return filters;
    }

    @Override
    public Filter deserialize(JSONObject json) throws JSONException {
        return null;
    }

}
