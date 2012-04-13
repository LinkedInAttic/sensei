package com.senseidb.search.client.json;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class JsonDeserializer {
    public static <T> T deserialize(Class<T> cls, JSONObject jsonObject) {
        return deserialize(cls, jsonObject, true);
    }


    public static <T> T deserialize(Class<T> cls, JSONObject jsonObject, boolean handleCustomJsonHandler) {
        try {
            if (jsonObject == null) {
                return null;
            }

            CustomJsonHandler customJsonHandler = (CustomJsonHandler) ReflectionUtil.getAnnotation(cls, CustomJsonHandler.class);
            if (customJsonHandler != null && handleCustomJsonHandler) {
                JsonHandler jsonHandler;
               try {
                  jsonHandler = customJsonHandler.value().newInstance();
               } catch (Exception e) {
                   throw new RuntimeException(e);
               }
               return (T) jsonHandler.deserialize(jsonObject);
           }
            T obj = cls.newInstance();
            for (Field field : cls.getDeclaredFields()) {
                field.setAccessible(true);
                Class<?> type = field.getType();
                String name = field.getName();
                if (field.isAnnotationPresent(JsonField.class)) {
                    name = field.getAnnotation(JsonField.class).value();
                }
                Type genericType = field.getGenericType();
                Object value = null;
                if (jsonObject.opt(name) == null) {
                    continue;
                }
                if (type == Integer.class) {
                    value = jsonObject.optInt(name);
                } else if (type.isPrimitive()) {
                	value = jsonObject.opt(name);
                }
                else if (type == Boolean.class) {
                    value = jsonObject.optBoolean(name);
                } else if (type == Long.class) {
                  try {
                    value = Long.parseLong(jsonObject.optString(name, "0"));
                  }
                  catch(Exception e) {
                    value = 0L;
                  }
                } else if (type == String.class) {
                    value = jsonObject.optString(name);
                } else if (type == Double.class) {
                    value = jsonObject.optDouble(name);
                } else if (type.isEnum()) {
                    value = jsonObject.optString(name);
                    if (value != null) {
                    	value = Enum.valueOf((Class)type, value.toString());
                    }
                } else if (type == List.class) {
                    JSONArray jsonArray = jsonObject.optJSONArray(name);
                    if (jsonArray == null) {
                        continue;
                    }
                    boolean isParameterizedType = genericType  instanceof ParameterizedType;
                    value = deserializeArray((isParameterizedType ? getGenericType(genericType, 0) : null), jsonArray);
                }  else if (type == Map.class) {

                		value = deserializeMap(genericType, jsonObject.getJSONObject(name));
                		if (value == null) {
                			continue;
                		}
                    }  else {
                    	JSONObject propObj = jsonObject.optJSONObject(name);
                    	if (propObj == null) {
                    		continue;
                    	}
                    	if (type == JSONObject.class) {
                    			value = propObj;
                    	} else {
                    		value = deserialize(type, propObj);
                    	}
                    }
                field.set(obj, value);
            }
            return obj;
        } catch (Exception ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }
    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static Map deserializeMap(Type genericType,  JSONObject mapJson ) throws Exception {
        Map map = new HashMap();

        if (mapJson == null) {
            return null;
        }
        String[] names = JSONObject.getNames(mapJson);
        if (names == null) {
            return null;
        }
        Type valueType =  getGenericType(genericType, 1);
        for (String paramName : names) {
            Object mapValue = mapJson.opt(paramName);
            if (mapValue == null) {
                map.put(paramName, null);
            } else if (mapValue instanceof JSONArray) {
                if (valueType instanceof ParameterizedType) {
                    map.put(paramName, deserializeArray(getGenericType(valueType, 0), (JSONArray) mapValue));
                } else {
                    map.put(paramName, deserializeArray(null, (JSONArray) mapValue));
                }
            } else if (valueType instanceof ParameterizedType && Map.class.isAssignableFrom(((Class)((ParameterizedType)valueType).getRawType()))) {

                map.put(paramName, deserializeMap(valueType, (JSONObject)mapValue));
            }  else if (mapValue instanceof JSONObject) {
                map.put(paramName, deserialize((Class)valueType, (JSONObject) mapValue));
            } else {
                map.put(paramName, mapValue);
            }
        }
       return map;
    }
    private static List deserializeArray(Type type, JSONArray jsonArray) throws JSONException {
        ArrayList value = new ArrayList(jsonArray.length());
        for (int i = 0; i < jsonArray.length(); i++) {
            Object elem = jsonArray.get(i);
            if (elem instanceof JSONObject) {
                ((List) value).add(deserialize((Class)type, (JSONObject) elem));
            } else {
                ((List) value).add(elem);
            }
        }
        return value;
    }

    private static Type getGenericType(Field field) {
        ParameterizedType paramType = (ParameterizedType) field.getGenericType();
        return paramType.getActualTypeArguments()[0];
    }
    private static Type getGenericType(Type cls, int paramIndex) {
        return ((ParameterizedType) cls).getActualTypeArguments()[paramIndex];

    }
}
