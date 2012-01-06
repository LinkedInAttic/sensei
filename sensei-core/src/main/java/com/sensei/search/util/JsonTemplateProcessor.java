package com.sensei.search.util;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

public class JsonTemplateProcessor {
  private static final String TEMPLATE_MAPPING_PARAM = "templateMapping";
  private final static Logger logger = Logger.getLogger(JsonTemplateProcessor.class);

  private Map<String, Object> getTemplates(JSONObject request) {
    Map<String, Object> ret = new HashMap<String, Object>();
    JSONObject templatesJson = request.optJSONObject(TEMPLATE_MAPPING_PARAM);
    if (templatesJson == null) {
      return ret;
    }
    Iterator keys = templatesJson.keys();
    while (keys.hasNext()) {
      String templateName = (String) keys.next();
      Object templateValueObj = templatesJson.opt(templateName);
      if (templateValueObj != null && (templateValueObj instanceof String || templateValueObj instanceof Number)) {
        ret.put(templateName, templateValueObj);
      } else {
        throw new UnsupportedOperationException("Value for the template " + templateName
            + " couldn't be transformed to the primitive type");
      }
    }

    return ret;
  }

  public JSONObject substituteTemplates(JSONObject request) {
    Map<String, Object> templates = getTemplates(request);
    if (templates.isEmpty()) {
      logger.debug("not templates found for substitution");
      return request;
    }
    //logger.info(String.format("Found %d templates to substitute - %s", templates.size(), templates.keySet()));
    String requestStr = request.toString();

    String modifiedStr = requestStr;
    for (String key : templates.keySet()) {
      String replaceable = "$" + key;
        Object value = templates.get(key);
        if (value == null) {
          continue;
        }
       boolean isString = value instanceof String;
       if (StringUtils.containsAny(value.toString(), "\n'\t><&$@#;")) {
         throw new IllegalArgumentException("The substitution template value for the key " + key + " contains special characters");
       }
       if (!isString) {

          replaceable = "\"" +replaceable + "\"";
        }


        modifiedStr = modifiedStr.replace(replaceable, value.toString());

    }
    if (modifiedStr.equals(requestStr)) {
      logger.info("No substitutions were made");
      return request;
    }
    try {
      return new JSONObject(modifiedStr);
    } catch (JSONException e) {
      throw new RuntimeException("After the substitution was made, the the object is not a valid json object");
    }
  }

}
