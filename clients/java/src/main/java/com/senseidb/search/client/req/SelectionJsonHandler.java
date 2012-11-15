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

package com.senseidb.search.client.req;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.json.JSONException;
import org.json.JSONObject;

import com.senseidb.search.client.json.JsonDeserializer;
import com.senseidb.search.client.json.JsonHandler;
import com.senseidb.search.client.json.JsonSerializer;

public class SelectionJsonHandler implements JsonHandler<Selection> {
    public static Map<String, Class<? extends Selection>> selectionClasses = new HashMap<String, Class<? extends Selection>>();
    static {
        for (Class<? extends Selection> cls : Arrays.asList(Term.class,
                Terms.class, Path.class, Range.class, Selection.Custom.class)) {
            selectionClasses.put(cls.getSimpleName().toLowerCase(), cls);
        }
    }

    @Override
    public JSONObject serialize(Selection bean) throws JSONException {
        if (bean == null) {
            return null;
        }
        if (bean instanceof Selection.Custom) {
            JSONObject ret = new JSONObject();
            ret.put("custom", ((Selection.Custom) bean).getCustom());
            return ret;
        }

        JSONObject innerObject = (JSONObject) JsonSerializer.serialize(bean,
                false);
        JSONObject paramContainer = new JSONObject();
        paramContainer.put(bean.getField(), innerObject);
        JSONObject ret = new JSONObject();
        ret.put(bean.getClass().getSimpleName().toLowerCase(), paramContainer);
        return ret;
    }

    @Override
    public Selection deserialize(JSONObject json) throws JSONException {
        String[] names = JSONObject.getNames(json);
        if (names.length == 0 || !selectionClasses.keySet().contains(names[0])) {
            throw new IllegalStateException(
                    "The json object doesn't contain the value from "
                            + Arrays.toString(names));
        }
        String name = names[0];
        JSONObject innerPart = json.getJSONObject(name);
        if ("custom".equals(name)) {
            return Selection.custom(innerPart);
        }
        String fieldName = JSONObject.getNames(innerPart)[0];
        Selection selection = JsonDeserializer.deserialize(
                selectionClasses.get(name), innerPart.getJSONObject(fieldName),
                false);
        selection.setField(fieldName);
        return selection;
    }

}
