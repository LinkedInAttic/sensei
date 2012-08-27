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

package com.senseidb.search.client.req.relevance;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The "values" part provides the input values for either the runtime model or the predefined one
 * values part is used for either predefined model or a runtime model above, if these models require input values;
 *   <pre>            "values":{
 *                   "thisYear":2001,
 *                   "goodYear":[
  *                      1996,
  *                      1997
  *                  ]
  *              }</pre>
 */
public class RelevanceValues {
  protected Map<String, Object> values = new HashMap<String, Object>();
  
  public static RelevanceValuesBuilder builder() {
    return new RelevanceValuesBuilder();
  }

  public Map<String, Object> getValues() {
    return values;
  }
private RelevanceValues() {
  // TODO Auto-generated constructor stub
}
  public static class RelevanceValuesBuilder {
    private RelevanceValues relevanceValues;

    public RelevanceValuesBuilder() {
      relevanceValues = new RelevanceValues();
    }

    public RelevanceValuesBuilder addListValue(String variableName, Object... values) {
      for (Object value : values) {
        checkType(value);
      }
      relevanceValues.values.put(variableName, Arrays.asList(values));
      return this;
    }

    public RelevanceValuesBuilder addAtomicValue(String variableName, Object value) {
      checkType(value);
      relevanceValues.values.put(variableName, value);
      return this;
    }

    private void checkType(Object value) {
      if (!(value instanceof String) && !(value instanceof Number)) {
        throw new IllegalStateException("The value should be either String or Number");
      }
    }
    public RelevanceValuesBuilder addMapValue(String variableName, List keys, List values) {     
      for (int i = 0; i < keys.size(); i++) {
        checkType(keys.get(i));
        checkType(values.get(i));
      }
      Map<String, Object> ret = new HashMap<String, Object>(2);
      ret.put("key", keys);
      ret.put("value", values);
      relevanceValues.values.put(variableName, ret);
      return this;
   }
    public RelevanceValuesBuilder addMapValue(String variableName, Map<Object, Object> valuesMap) {
      List<Object> keys = new ArrayList<Object>(valuesMap.size());
      List<Object> values = new ArrayList<Object>(valuesMap.size());
      for (Map.Entry<Object, Object> entry : valuesMap.entrySet()) {
        checkType(entry.getKey());
        checkType(entry.getValue());
        keys.add(entry.getKey());
        values.add(entry.getValue());
      }
      Map<String, Object> ret = new HashMap<String, Object>(2);
      ret.put("key", keys);
      ret.put("value", values);
      relevanceValues.values.put(variableName, ret);
      return this;
    }
    public RelevanceValues build() {
      return relevanceValues;
    }
  }
}
