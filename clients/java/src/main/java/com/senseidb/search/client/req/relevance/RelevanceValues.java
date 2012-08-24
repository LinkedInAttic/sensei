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
