package com.senseidb.search.client.json.req.filter;

public class IsNull implements Filter {
  private String field;

  public IsNull(String field) {
    super();
    this.field = field;
  }

  public String getField() {
    return field;
  }

  public void setField(String field) {
    this.field = field;
  }
  
} 
