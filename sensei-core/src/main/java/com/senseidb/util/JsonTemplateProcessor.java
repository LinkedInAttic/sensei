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
package com.senseidb.util;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.apache.log4j.Logger;
public class JsonTemplateProcessor{
  public static final String TEMPLATE_MAPPING_PARAM = "templateMapping";
  private final static Logger logger = Logger.getLogger(JsonTemplateProcessor.class);

  public Map<String, Object> getTemplates(JSONObject request) {
    Map<String, Object> ret = new HashMap<String, Object>();
    JSONObject templatesJson = request.optJSONObject(TEMPLATE_MAPPING_PARAM);
    if (templatesJson == null) {
      return ret;
    }
    Iterator keys = templatesJson.keys();
    while (keys.hasNext()) {
      String templateName = (String) keys.next();
      Object templateValueObj = templatesJson.opt(templateName);
      if (templateValueObj != null &&
          (templateValueObj instanceof String ||
           templateValueObj instanceof Number ||
           templateValueObj instanceof JSONArray)) {
        ret.put(templateName, templateValueObj);
      } else {
        throw new UnsupportedOperationException("Value for the template " + templateName
            + " couldn't be transformed to a primitive type or JSONArray");
      }
    }

    return ret;
  }
 
  public JSONObject substituteTemplates(JSONObject request) {
    try {
      return (JSONObject) process(request, getTemplates(request));
    } catch (JSONException ex) {
      throw new RuntimeException(ex);
    }
  }

  public Object process(Object src, Map<String, Object> templates) throws JSONException {
    if (src instanceof String) {
      return processString((String) src, templates);
    }
    if (src instanceof JSONObject) {
      return processJsonObject((JSONObject) src, templates);
    }
    if (src instanceof JSONArray) {
      JSONArray arr = (JSONArray) src;
      for (int i = 0; i < arr.length(); i++) {
        arr.put(i, process(arr.get(i), templates));
      }
      return arr;
    }
    return src;
  }

  private JSONObject processJsonObject(JSONObject src, Map<String, Object> templates) throws JSONException {
    if (src == null) {
      return null;
    }
    String[] names = JSONObject.getNames(src);
    if (names == null || names.length == 0) {
      return src;
    }
    for (String name : names) {
      Object val = process(src.get(name), templates);
      Object newName = processString(name, templates);
      if (newName != name) {
        src.remove(name);
      }
      src.put(newName.toString(), val);
    }
    return src;
  }

  private Object processString(String src, Map<String, Object> templates) {
    if (!src.contains("$")) {
      return src;
    }
    for (String key : templates.keySet()) {
      String replaceable = "$" + key;
      Object value = templates.get(key);
      if (value == null) {
        continue;
      }
      if (src.equals(replaceable)) {
        if (value instanceof String) {
          value = ((String) value).replaceAll("\\$\\$", "\\$");
        }
        return value;
      }

      int index = -1;
      while ((index = src.indexOf(replaceable, index + 1)) >= 0) {
        int numSigns = numPrecedingDollarSigns(src, index);
        if (numSigns % 2 == 1) {
          src = src.substring(0, index) + value.toString() + src.substring(index + replaceable.length());
        }
      }
    }
    src = src.replaceAll("\\$\\$", "\\$");
    return src;
  }

  private int numPrecedingDollarSigns(String replaceable, int index) {
    int ret = 0;
    while (index >= 0 && replaceable.charAt(index) == '$') {
      ret++;
      index--;
    }
    return ret;
  }
}
