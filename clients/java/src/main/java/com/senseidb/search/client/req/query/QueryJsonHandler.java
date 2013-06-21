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

package com.senseidb.search.client.req.query;

import java.util.HashMap;
import java.util.Map;

import org.json.JSONException;
import org.json.JSONObject;

import com.senseidb.search.client.json.JsonHandler;
import com.senseidb.search.client.json.JsonSerializer;
import com.senseidb.search.client.req.Range;
import com.senseidb.search.client.req.Selection;
import com.senseidb.search.client.req.Term;
import com.senseidb.search.client.req.Terms;
import com.senseidb.search.client.req.filter.Ids;
import com.senseidb.search.client.req.query.span.SpanFirst;
import com.senseidb.search.client.req.query.span.SpanNear;
import com.senseidb.search.client.req.query.span.SpanNot;
import com.senseidb.search.client.req.query.span.SpanOr;
import com.senseidb.search.client.req.query.span.SpanTerm;

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
            throw new UnsupportedOperationException(
                    "Class "
                            + bean.getClass()
                            + " is not supported for serialization by the QueryJsonHandler");
        }
        JSONObject defaultSerialization = (JSONObject) JsonSerializer
                .serialize(bean, false);
        if (bean instanceof FieldAwareQuery) {
            defaultSerialization.remove("field");
            if (bean instanceof SpanTerm
                    && ((SpanTerm) bean).getBoost() == null) {
                SpanTerm spanTerm = (SpanTerm) bean;
                defaultSerialization = new JSONObject().put(
                        spanTerm.getField(), spanTerm.getValue());
            } else {
                defaultSerialization = new JSONObject().put(
                        ((FieldAwareQuery) bean).getField(),
                        defaultSerialization);
            }
        }
        if (bean instanceof Selection) {
            defaultSerialization.remove("field");
            defaultSerialization = new JSONObject().put(
                    ((Selection) bean).getField(), defaultSerialization);
        }
        if (bean.getRelevance() != null) {
            defaultSerialization.remove("relevance");
            defaultSerialization.put("relevance",
                    JsonSerializer.serialize(bean.getRelevance()));
        }
        return new JSONObject().put(typeNames.get(bean.getClass()),
                defaultSerialization);
    }

    @Override
    public Query deserialize(JSONObject json) throws JSONException {

        return null;
    }
}
