package com.senseidb.search.client.req;

import java.util.List;

import com.senseidb.search.client.json.JsonField;

public class RequestMetadata {
  @JsonField("select_list")
  private List<String> shownOnlyFields;

  public RequestMetadata(List<String> shownOnlyFields) {
    super();
    this.shownOnlyFields = shownOnlyFields;
  }
  
}
