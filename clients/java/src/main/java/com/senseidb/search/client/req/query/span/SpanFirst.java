package com.senseidb.search.client.req.query.span;

import com.senseidb.search.client.json.CustomJsonHandler;
import com.senseidb.search.client.req.query.Query;
import com.senseidb.search.client.req.query.QueryJsonHandler;

/**
 * <p>
 * Matches spans near the beginning of a field. The span first query maps to
 * Sensei <code>SpanFirstQuery</code>. Here is an example:
 * </p>
 *
  *
 * <p>
 * The <code>match</code> clause can be any other span type query. The
 * <code>end</code> controls the maximum end position permitted in a match.
 * </p>
 *
 *
 */
@CustomJsonHandler(QueryJsonHandler.class)
public class SpanFirst extends Query {

  SpanTerm match;
  int end;
  private double boost;

  public SpanFirst(SpanTerm match, int end, double boost) {
    super();
    this.match = match;
    this.end = end;
    this.boost = boost;
    match.setBoost(null);
  }

}
