package com.senseidb.search.client.req.query;

public class FieldAware {
  protected String field;

  public FieldAware setField(String field) {
    this.field = field;
    return this;
  }

  public String getField() {
    return field;
  }

}
