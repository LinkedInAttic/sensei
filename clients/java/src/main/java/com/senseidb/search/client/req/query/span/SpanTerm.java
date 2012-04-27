package com.senseidb.search.client.req.query.span;

import java.util.List;

import com.senseidb.search.client.json.CustomJsonHandler;
import com.senseidb.search.client.req.query.FieldAwareQuery;
import com.senseidb.search.client.req.query.QueryJsonHandler;

/**
 * <p>
 * Matches spans containing a term. The span term query maps to Sensei
 * <code>SpanTermQuery</code>. Here is an example:
 */
@CustomJsonHandler(QueryJsonHandler.class)
public class SpanTerm extends FieldAwareQuery {
  private String value;
  private Double boost;

  public SpanTerm(String field, String value, Double boost) {
    super();
    this.value = value;
    this.boost = boost;
    this.field = field;
  }

  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value;
  }

  public Double getBoost() {
    return boost;
  }

  public void setBoost(Double boost) {
    this.boost = boost;
  }

  public static void cleanBoosts(List<SpanTerm> spanTerms) {
    for (SpanTerm spanTerm : spanTerms) {
      spanTerm.setBoost(null);
    }
  }

}
