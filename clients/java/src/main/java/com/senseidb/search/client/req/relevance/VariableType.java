package com.senseidb.search.client.req.relevance;

/**
 * Supported variable types:
 * <ul>
 * <li>Variable type:
 * <li>HashSet: in detail, set_int, set_float, set_string, set_double,
 * set_long|set_int, set_float, set_string, set_double, set_long.
 * <li>HashMap: e.g., map_int_int, map_int_double, map_int_float,
 * map_string_int, map_string_double, etc. (Currently support two types hashmap:
 * map_int_* and map_string_*)
 * <li>other normal type: int, double, float, long, bool, string.
 * </ul>
 * 
 * 
 */
public enum VariableType {
  set_int, set_float, set_string, set_double, set_long, map_int_int, map_int_double, map_int_float, map_int_long, map_int_bool, map_int_string, map_string_int, map_string_double, map_string_float, map_string_long, map_string_bool, map_string_string, type_int, type_double, type_float, type_long, type_bool, type_string;
  public String getValue() {
    if (this.name().startsWith("type_")) {
      return this.name().substring("type_".length());
    }
    return this.name();
  }
}
