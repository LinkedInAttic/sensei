package com.senseidb.util;

import com.senseidb.search.query.BooleanQueryConstructor;
import com.senseidb.search.query.QueryConstructor;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Searchable;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


/**
 * Utility class for constructing custom objects using object constructors
 * or static builder methods within the class. Primarily used in construction
 * of custom query and custom collector objects.
 * @author darya
 */
public class ObjectContructorUtil {

  public static final String CONSTRUCTOR = "constructor";
  public static final String STATIC_METHOD = "static_method";
  public static final String USE_DEFAULT_SEARCHABLE = "use_default_searchable";
  public static final String USE_CONSTRUCTED_QUERY = "use_constructed_query";
  public static final String METHOD_NAME = "method_name";
  public static final String NUM_ARGS = "num_args";
  public static final String ARGS = "args";
  public static final String TYPE = "type";
  public static final String VALUE = "value";
  public static final String TYPE_INT = "int";
  public static final String TYPE_BOOLEAN = "boolean";
  public static final String TYPE_FLOAT = "float";
  public static final String TYPE_DOUBLE = "double";
  public static final String TYPE_QUERY = "query";
  public static final String TYPE_MAP = "map";
  public static final String TYPE_LIST = "list";
  public static final String TYPE_STRING = "string";
  public static final String TYPE_ARRAY = "array";
  public static final String TYPE_CLASS = "class";
  public static final String TYPE_ENUM = "enum";
  public static final String VALUE_NULL = "null";
  public static final String SEPERATER = ":";
  public static final String COMMA = ",";
  public static final String ARRAY_VALUE_SEPERATOR = "^V";


  public static Object constructObject(String className,
                                       JSONObject jsonQuery,
                                       QueryParser queryParser,
                                       Query q,
                                       Searchable searchable)
  throws JSONException,
         ClassNotFoundException,
         IllegalAccessException,
         InstantiationException,
         NoSuchMethodException,
         InvocationTargetException,
         NoSuchFieldException {

    JSONObject classConstructor = jsonQuery.optJSONObject(CONSTRUCTOR);
    JSONObject methodCall = jsonQuery.optJSONObject(STATIC_METHOD);
    int numArgs = 0;

    boolean isContructor = classConstructor == null ? false : true;
    boolean isMethod = methodCall == null ? false : true;
    JSONObject argumentsJson = null;

    if (isContructor) {
      numArgs = classConstructor.optInt(NUM_ARGS);
      argumentsJson = classConstructor.optJSONObject(ARGS);
    } else if (isMethod) {
      numArgs = methodCall.optInt(NUM_ARGS);
      argumentsJson = methodCall.optJSONObject(ARGS);
    }

    if (isContructor && numArgs == 0) {
      return Class.forName(className).newInstance();
    }


    Iterator argsIterator = argumentsJson.keys();

    Map<Integer, Object> arguments = new HashMap<Integer, Object>();
    Map<Integer, Class> argumentsType = new HashMap<Integer, Class>();

    while (argsIterator.hasNext()) {
      Integer argIndex =  Integer.parseInt((String) argsIterator.next());
      JSONObject argInfo = (JSONObject) argumentsJson.get(argIndex.toString());
      String objectType = (String) argInfo.get(TYPE);
      String objectValue = null;

      if (objectType.equals(TYPE_BOOLEAN)) {
        objectValue = (String) argInfo.get(VALUE);
        arguments.put(argIndex, Boolean.parseBoolean(objectValue));
        argumentsType.put(argIndex, boolean.class);
      }
      else if (objectType.equals(TYPE_INT)) {
        objectValue = (String) argInfo.get(VALUE);
        arguments.put(argIndex, Integer.parseInt(objectValue));
        argumentsType.put(argIndex, int.class);
      }
      else if (objectType.equals(TYPE_FLOAT)) {
        objectValue = (String) argInfo.get(VALUE);
        arguments.put(argIndex, Float.parseFloat(objectValue));
        argumentsType.put(argIndex, float.class);
      }
      else if (objectType.equals(TYPE_DOUBLE)) {
        objectValue = (String) argInfo.get(VALUE);
        arguments.put(argIndex, Double.parseDouble(objectValue));
        argumentsType.put(argIndex, double.class);
      }
      else if (objectType.equals(TYPE_STRING)) {
        objectValue = (String) argInfo.get(VALUE);
        if (!objectValue.equals(VALUE_NULL)) {
          arguments.put(argIndex, objectValue);
        }
        else {
          arguments.put(argIndex, null);
        }
        argumentsType.put(argIndex, String.class);
      }
      else if (objectType.equals(TYPE_MAP)) {
        Map<Object, Object> objectMap = new HashMap<Object, Object>();

        if(argInfo.getString(VALUE).equals(VALUE_NULL)) {
          arguments.put(argIndex, null);
          argumentsType.put(argIndex, Map.class);
          continue;
        }

        JSONObject jsonMap = argInfo.getJSONObject(VALUE);
        Iterator mapIter = jsonMap.keys();
        while(mapIter.hasNext()) {
          String jKey = (String) mapIter.next();
          objectMap.put(jKey, jsonMap.get(jKey));
        }

        arguments.put(argIndex, objectMap);
        argumentsType.put(argIndex, Map.class);
      }
      else if (objectType.equals(TYPE_LIST)) {
        List<String> list = new ArrayList<String>();
        JSONArray jsonArray = argInfo.getJSONArray(VALUE);

        for (int i = 0; i < jsonArray.length(); i++) {
          list.add(jsonArray.getString(i));
        }

        arguments.put(argIndex, list);
        argumentsType.put(argIndex, List.class);
      }
      else if (objectType.contains(TYPE_ARRAY + SEPERATER)) {
        objectValue = (String) argInfo.get(VALUE);
        String[] split = objectType.split(SEPERATER);
        String[] cVals = objectValue.split("\\" + ARRAY_VALUE_SEPERATOR);
        String cName = split[1];
        Integer count = Integer.parseInt(split[3]);
        Class objectClass = Class.forName(cName);
        Class arrayClass = Array.newInstance(objectClass, count).getClass();
        Object o = Array.newInstance(objectClass, count);
        for (int i = 0; i < count; i++) {
          JSONObject v = new JSONObject(cVals[i]);
          Object val = constructObject(cName, v, queryParser, q, searchable);
          Array.set(o, i, val);
        }

        Object[] oo = (Object[])o;
        arguments.put(argIndex, oo);

        argumentsType.put(argIndex, arrayClass);
      }
      else if (objectType.equals(TYPE_QUERY)) {
        objectValue = (String) argInfo.get(VALUE);
        JSONObject qTmp = new JSONObject(objectValue);
        qTmp = qTmp.optJSONObject(TYPE_QUERY).optJSONObject(BooleanQueryConstructor.QUERY_TYPE);
        QueryConstructor queryConstructor =
            QueryConstructor.getQueryConstructor(BooleanQueryConstructor.QUERY_TYPE, queryParser);
        Query baseQuery = queryConstructor.doConstructQuery(qTmp);
        arguments.put(argIndex, baseQuery);
        argumentsType.put(argIndex, Query.class);
      }
      else if (objectType.contains(TYPE_CLASS + SEPERATER)) {
        objectValue = (String) argInfo.get(VALUE);
        String[] split = objectType.split(SEPERATER);
        String cName = split[1];
        Class objectClass = Class.forName(cName);
        boolean useSuperClass = Boolean.parseBoolean(split[2]);

        if (useSuperClass) {
          Class c = objectClass;
          List<Class> classList = new ArrayList<Class>();
          while(c != null) {
            classList.add(c);
            c = c.getSuperclass();
          }

          objectClass = classList.get(classList.size() - 2);
        }

        if (objectValue.equals(VALUE_NULL)) {
          arguments.put(argIndex, null);
          argumentsType.put(argIndex, objectClass);
          continue;
        }
        else if (objectValue.equals(USE_DEFAULT_SEARCHABLE)) {
          arguments.put(argIndex, searchable);
          argumentsType.put(argIndex, Searchable.class);
          continue;
        }
        else if (objectValue.equals(USE_CONSTRUCTED_QUERY)) {
          arguments.put(argIndex, q);
          argumentsType.put(argIndex, Query.class);
          continue;
        }

        Object cObj = constructObject(cName, new JSONObject(objectValue), queryParser, q, searchable);

        arguments.put(argIndex, cObj);
        argumentsType.put(argIndex, objectClass);
      }
      else if (objectType.contains(TYPE_ENUM + SEPERATER)) {
        objectValue = (String) argInfo.get(VALUE);
        String[] split = objectType.split(SEPERATER);
        String cName = split[1];
        boolean useSuperClass = Boolean.parseBoolean(split[2]);
        Class objectClass = Class.forName(cName);

        if (useSuperClass) {
          Class c = objectClass;
          List<Class> classList = new ArrayList<Class>();
          while(c != null) {
            classList.add(c);
            c = c.getSuperclass();
          }

          objectClass = classList.get(classList.size() - 2);
        }

        if (objectClass.isEnum()) {
          Object[] enumConstants = objectClass.getEnumConstants();
          for (Object o : enumConstants) {
            if (objectValue.equals(o.toString())) {
              arguments.put(argIndex, o);
              argumentsType.put(argIndex, objectClass);
              continue;
            }
          }
        }
      }
    }

    Class [] argClasses = new Class[argumentsType.size()];
    Object [] argValues = new Object[arguments.size()];

    for (Map.Entry<Integer, Class> entry : argumentsType.entrySet()) {
      argClasses[entry.getKey() - 1] = entry.getValue();
    }

    for (Map.Entry<Integer, Object> entry : arguments.entrySet()) {
      argValues[entry.getKey() - 1] = entry.getValue();
    }

    Class x = Class.forName(className);

    if (isContructor) {
      Constructor xConstructor = x.getConstructor(argClasses);
      Object obj = xConstructor.newInstance(argValues);
      return obj;
    }
    else if (isMethod) {
      String methodName = methodCall.optString(METHOD_NAME);
      Method staticMethod = x.getMethod(methodName, argClasses);
      Object obj = staticMethod.invoke(null, argValues);
      return obj;
    }

    return null;
  }

}
