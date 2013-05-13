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
           templateValueObj instanceof JSONArray ||
           templateValueObj instanceof JSONObject)) {
        ret.put(templateName, templateValueObj);
      } else {
        throw new UnsupportedOperationException("Value for the template " + templateName
            + " couldn't be transformed to a primitive type, JSONArray, or JSONObject");
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
