package com.senseidb.search.client;

import org.json.JSONException;
import org.json.JSONObject;

import java.util.Map;


/**
 * Utility class used to construct Json representation
 * of custom objects. Primarily used for Json representation
 * of custom query and collectors.
 */
public class CustomObjConstructor {

  private static final String ARGUMENT_TYPE = "type";
  private static final String ARGUMENT_VALUE = "value";
  private static final String NUM_ARGS = "num_args";
  private static final String ARGUMENTS = "args";
  private static final String CONSTRUCT_TYPE = "constructor";
  private static final String METHOD_TYPE = "static_method";
  private static final String METHOD_NAME = "method_name";


  public static JSONObject buildJsonObject(boolean isStaticMethod,
                                           Map<Integer,Map<String, String>> argumentsMap,
                                           String methodName) throws JSONException {

    JSONObject objDescription = new JSONObject();
    JSONObject argument = new JSONObject();

    for (Map.Entry<Integer, Map<String, String>> entry : argumentsMap.entrySet()) {
      if (entry.getValue().size() != 1) {
        throw new IllegalArgumentException("Argument description must be a singleton map.");
      }
      Map.Entry<String, String> m = entry.getValue().entrySet().iterator().next();
      JSONObject argumentDescription = new JSONObject();
      argumentDescription.put(ARGUMENT_TYPE, m.getKey());
      argumentDescription.put(ARGUMENT_VALUE, m.getValue());
      argument.put(entry.getKey().toString(), argumentDescription);
    }
    objDescription.put(NUM_ARGS, argumentsMap.size());
    objDescription.put(ARGUMENTS, argument);
    if (isStaticMethod) {
      if (methodName == null || methodName.length() == 0) {
        throw new IllegalArgumentException("Method name is invalid.");
      }
      objDescription.put(METHOD_NAME, methodName);
    }

    return objDescription;
  }

}
