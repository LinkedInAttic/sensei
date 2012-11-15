/**
 * This software is licensed to you under the Apache License, Version 2.0 (the
 * "Apache License").
 *
 * LinkedIn's contributions are made under the Apache License. If you contribute
 * to the Software, the contributions will be deemed to have been made under the
 * Apache License, unless you expressly indicate otherwise. Please do not make any
 * contributions that would be inconsistent with the Apache License.
 *
 * You may obtain a copy of the Apache License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, this software
 * distributed under the Apache License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the Apache
 * License for the specific language governing permissions and limitations for the
 * software governed under the Apache License.
 *
 * © 2012 LinkedIn Corp. All Rights Reserved.  
 */

package com.senseidb.search.client.req.filter;

import java.util.ArrayList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.senseidb.search.client.json.JsonHandler;
import com.senseidb.search.client.json.JsonSerializer;
import com.senseidb.search.client.req.Selection;
import com.senseidb.search.client.req.SelectionJsonHandler;
import com.senseidb.search.client.req.filter.Filter.AndOr;
import com.senseidb.search.client.req.query.Query;
import com.senseidb.search.client.req.query.QueryJsonHandler;
import com.senseidb.search.client.req.query.StringQuery;

public class FilterJsonHandler implements JsonHandler<Filter> {
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
                ret.put("must_not",
                        new JSONArray(convertToJson(bool.getMust_not())));
            }
            if (bool.getShould() != null) {
                ret.put("should",
                        new JSONArray(convertToJson(bool.getShould())));
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
                ret.put("excludes", new JSONArray(ids.getExcludes()));
            }

            return new JSONObject().put("ids", ret);
        }
        if (bean instanceof IsNull) {
            IsNull isNull = (IsNull) bean;

            return new JSONObject().put("isNull",
                    new JSONObject().put("field", isNull.getField()));
        }
        if (bean instanceof Query) {
            return queryJsonHandler.serialize((Query) bean);
        }
        if (bean instanceof QueryFilter) {
            return new JSONObject()
                    .put("query", queryJsonHandler
                            .serialize(((QueryFilter) bean).getQuery()));
        }
        throw new UnsupportedOperationException(bean.getClass()
                + " is not supported");

    }

    private List<JSONObject> convertToJson(List<Filter> filters2)
            throws JSONException {
        List<JSONObject> filters = new ArrayList<JSONObject>(filters2.size());
        for (Filter filter : filters2) {
            filters.add(serialize(filter));
        }
        return filters;
    }

    @Override
    public Filter deserialize(JSONObject json) throws JSONException {
        return null;
    }

}
