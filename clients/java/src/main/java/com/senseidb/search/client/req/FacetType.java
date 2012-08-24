package com.senseidb.search.client.req;

/**
 * Specifies different facet types
 * Supported facet types:

 *
 */
public enum FacetType {
   type_int, type_long, type_double, type_float, type_short, type_string;
   
   
   public String getValue() {
     return this.name().substring("type_".length());
   }
}
