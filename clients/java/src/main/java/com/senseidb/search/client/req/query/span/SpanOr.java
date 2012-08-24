package com.senseidb.search.client.req.query.span;

import java.util.List;

import com.senseidb.search.client.json.CustomJsonHandler;
import com.senseidb.search.client.req.query.Query;
import com.senseidb.search.client.req.query.QueryJsonHandler;

/**
 * <p>
 * Matches the union of its span clauses. The span or query maps to Sensei
 * <code>SpanOrQuery</code>. Here is an example:
 * </p>
 *
 *
 * <p>
 * The <code>clauses</code> element is a list of one or more other span type
 * queries.
 * </p>
 *
 *
 */
@CustomJsonHandler(QueryJsonHandler.class)
public class SpanOr extends Query {
  List<SpanTerm> clauses;
  private final Double boost;

  public SpanOr(List<SpanTerm> clauses, Double boost) {
    super();
    this.clauses = clauses;
    SpanTerm.cleanBoosts(clauses);
    this.boost = boost;
  }

}
