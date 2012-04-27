package com.senseidb.search.client.req.relevance;

import com.senseidb.search.client.json.CustomJsonHandler;

public class Relevance {
  private Model model;
  @CustomJsonHandler(value = RelevanceValuesHandler.class)
  private RelevanceValues values;
  private Relevance() {
    // TODO Auto-generated constructor stub
  }
  public static Relevance valueOf(Model model, RelevanceValues values) {
    Relevance relevance = new Relevance();
    relevance.model = model;
    relevance.values = values;
    return relevance;
  }
}
