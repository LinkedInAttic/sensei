package com.senseidb.test.bql.parsers;

/* EXPERIMENTAL (really) */
/* Copyright (c) 2009 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import java.lang.Math;

import java.util.Iterator;

/**
 * A comparator for comparing json objects. This comparator takes care of
 * reordered elements in JSONArray. {@link #STRICT} mode means the two given
 * elements must match exactly, while in {@link #SIMPLE} mode extra elements in
 * the latter is ignored.
 *
 * Check unittests {@link JsonComparatorTest} for examples on this.
 *
 * @author Sachin Shenoy
 */
public class JsonComparator {

  public static final int STRICT = 1;
  public static final int SIMPLE = 2;
  private final int policy;
 
  public JsonComparator(int policy) {
    this.policy = policy;
  }
 
  /**
   * Compares two json objects.
   *
   * @param a first element to compare
   * @param b second element to compare
   * @return true if the elements are equal as per the policy.
   */
  public boolean isEquals(Object a, Object b) {
    if (a instanceof JSONObject) {
      if (b instanceof JSONObject) {
        return isEqualsJsonObject((JSONObject) a, (JSONObject) b);
      } else {
        return false;
      }
    }

    if (a instanceof JSONArray) {
      if (b instanceof JSONArray) {
        return isEqualsJsonArray((JSONArray) a, (JSONArray) b);
      } else {
        return false;
      }
    }
   
    if (a instanceof Long) {
      if (b instanceof Long) 
      {
        return isEqualsLong((Long) a, (Long) b);
      }
      else if (b instanceof Integer)
      {
        return isEqualsLong((Long) a,  new Long((long) ((Integer) b).intValue()));
      }
      else {
        return false;
      }
    }

    if (a instanceof Integer) {
      if (b instanceof Integer) {
        return isEqualsInteger((Integer) a, (Integer) b);
      }
      else if (b instanceof Long)
      {
        return isEqualsInteger((Integer) a, new Integer((int) ((Long) b).longValue()));
      }
      else {
        return false;
      }
    }

    if (a instanceof String) {
      if (b instanceof String) {
        return isEqualsString((String) a, (String) b);
      } else {
        return false;
      }
    }
   
    if (a instanceof Boolean) {
      if (b instanceof Boolean) {
        return isEqualsBoolean((Boolean) a, (Boolean) b);
      } else {
        return false;
      }
    }

    if (a instanceof Float || a instanceof Double) {
      double val1 = (a instanceof Float)? ((Float) a).doubleValue() : ((Double) a).doubleValue();
      if (b instanceof Float || b instanceof Double) {
        double val2 = (b instanceof Float)? ((Float) b).doubleValue() : ((Double) b).doubleValue();
        return (Math.abs(val1-val2) < 0.001);
      } else {
        return false;
      }
    }
   
    if (a == null && b == null){
      return true;
    }
   
    if (a != null && b != null) {
      return a.equals(b);
    }
   
    return false;
  }

  private boolean isEqualsBoolean(Boolean a, Boolean b) {
    if (a == null ^ b == null) {
      return false;
    } else if (a == null && b == null) {
      return true;
    }
    return a.equals(b);
  }

  private boolean isEqualsString(String a, String b) {
    if (a == null ^ b == null) {
      return false;
    } else if (a == null && b == null) {
      return true;
    }
    return a.equals(b);
  }

  private boolean isEqualsLong(Long a, Long b) {
    if (a == null ^ b == null) {
      return false;
    } else if (a == null && b == null) {
      return true;
    }
    return a.equals(b);
  }

  private boolean isEqualsInteger(Integer a, Integer b) {
    if (a == null ^ b == null) {
      return false;
    } else if (a == null && b == null) {
      return true;
    }
    return a.equals(b);
  }

  private boolean isEqualsJsonArray(JSONArray a, JSONArray b) {
    if (policy == STRICT) {
      if (a.length() != b.length()) {
        return false;
      }
    }
   
    if (policy == SIMPLE) {
      if (a.length() > b.length()) {
        return false;
      }
    }
   
    boolean[] am = new boolean[a.length()];
    boolean[] bm = new boolean[b.length()];
   
    for (int i = 0; i < a.length(); ++i) if (am[i] == false) {
      for (int j = 0; j < b.length(); ++j) if (bm[j] == false) {
        try {
          if (isEquals(a.get(i), b.get(j))) {
            am[i] = true;
            bm[j] = true;
            break;
          }
        } catch (JSONException e) {
          e.printStackTrace();
        }
      }
    }
   
    for (int i = 0; i < am.length; ++i) if (!am[i]) {
      return false;
    }
   
    if (policy == STRICT) {
      for (int j = 0; j < bm.length; ++j) if (!bm[j]) {
        return false;
      }
    }

    return true;
  }

  private boolean isEqualsJsonObject(JSONObject a, JSONObject b) {
    if (policy == STRICT) {
      if (a.length() != b.length()) {
        return false;
      }
    }
   
    if (policy == SIMPLE) {
      if (a.length() > b.length()) {
        return false;
      }
    }

    Iterator keys = a.keys();
    while (keys.hasNext()) {
      String key = (String) keys.next();
      if (!b.has(key)) {
        return false;
      }
      try {
        if (!isEquals(a.get(key), b.get(key))) {
          return false;
        }
      } catch (JSONException e) {
        return false;
      }
    }
   
    return true;
  }
}
