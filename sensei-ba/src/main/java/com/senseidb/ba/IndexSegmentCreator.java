package com.senseidb.ba;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.json.JSONException;
import org.json.JSONObject;

public class IndexSegmentCreator {
    public static IndexSegment convert(String[] jsonDocs, Set<String> excludedColumns)  {
      Map<String, List> columnValues = new HashMap<String, List>();
      try {
      Map<String, Class<?>> columnTypes = getColumnTypes(jsonDocs, excludedColumns, 10);
      for (String column : columnTypes.keySet()) {
        columnValues.put(column, new ArrayList(jsonDocs.length));
      }
      for (String jsonDocStr : jsonDocs) {        
        
        JSONObject jsonDoc = new JSONObject(jsonDocStr);
        for (String column : columnTypes.keySet()) {
          Object value = jsonDoc.opt(column);
          if (value instanceof Integer && columnTypes.get(column) == long.class) {
            value = Long.valueOf((Integer) value);
          }
          columnValues.get(column).add(value);
        }
      } 
      IndexSegmentImpl offlineSegmentImpl = new IndexSegmentImpl(); 
      for (String column : columnTypes.keySet()) {
        ForwardIndexBackedByArray forwardIndexBackedByArray = new ForwardIndexBackedByArray(column);
        Class<?> type = columnTypes.get(column);
        if (type == int.class) {
          forwardIndexBackedByArray.initByIntValues(columnValues.get(column));
        } else if (type == long.class) {
          forwardIndexBackedByArray.initByLongValues(columnValues.get(column));
        } else if (type == String.class) {
          forwardIndexBackedByArray.initByStringValues(columnValues.get(column));
        }
        offlineSegmentImpl.forwardIndexes.put(column, forwardIndexBackedByArray);
        offlineSegmentImpl.dictionaries.put(column, forwardIndexBackedByArray.getDictionary());
      }
      offlineSegmentImpl.length = jsonDocs.length;
      offlineSegmentImpl.setColumnTypes(columnTypes);
      return offlineSegmentImpl;
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }

    public static Map<String, Class<?>> getColumnTypes(String[] jsonDocs, Set<String> excludedColumns, int mod)
        throws JSONException {
      Map<String, Class<?>> columnTypes = new HashMap<String, Class<?>>();
      int i = 0; 
      for (String jsonDocStr : jsonDocs) {
        if (i++ % mod != 0) {
          continue;
        }
        JSONObject jsonDoc = new JSONObject(jsonDocStr);
        if (jsonDoc == null) {
          throw new IllegalStateException();
        }
        Iterator keys = jsonDoc.keys();
        while (keys.hasNext()) {
          String key = (String) keys.next();
          if (excludedColumns.contains(key)) {
            continue;
          }
          if (!columnTypes.containsKey(key) || columnTypes.get(key) == int.class) {
            Object object = jsonDoc.get(key);
             if (columnTypes.get(key) == int.class && object instanceof Long) {
               columnTypes.put(key, long.class);
             } else {            
                if (object instanceof String) columnTypes.put(key, String.class);
                if (object instanceof Integer) columnTypes.put(key, int.class);
                if (object instanceof Long) columnTypes.put(key, long.class);
             }
          }
        }
      }
      return columnTypes;
    }
}
