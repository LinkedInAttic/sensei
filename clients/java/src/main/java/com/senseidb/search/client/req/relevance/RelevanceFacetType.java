package com.senseidb.search.client.req.relevance;

public enum RelevanceFacetType {
  type_int, type_long, type_double, type_float, type_short, type_string,
  type_mint, type_mlong, type_mdouble, type_mfloat, type_mshort, type_mstring, 
  type_wmint, type_wmlong, type_wmdouble, type_wmfloat, type_wmshort, type_wmstring;
  public String getValue() {
    return this.name().substring("type_".length());
  }
}
