package com.senseidb.search.client.req.query.span;

import java.util.List;

import com.senseidb.search.client.json.CustomJsonHandler;
import com.senseidb.search.client.json.JsonField;
import com.senseidb.search.client.req.query.Query;
import com.senseidb.search.client.req.query.QueryJsonHandler;

/**
 * <p>
 * Matches spans which are near one another. One can specify <em>slop</em>, the
 * maximum number of intervening unmatched positions, as well as whether matches
 * are required to be in-order. The span near query maps to Sensei
 * <code>SpanNearQuery</code>. Here is an example:
 * </p>
 *
 *
 * <p>
 * The <code>clauses</code> element is a list of one or more other span type
 * queries and the <code>slop</code> controls the maximum number of intervening
 * unmatched positions permitted.
 * </p>
 *
 *
 */
@CustomJsonHandler(QueryJsonHandler.class)
public class SpanNear extends Query {
  List<SpanTerm> clauses;
  private int slop;
  @JsonField("in_order")
  private boolean inOrder;
  @JsonField("collect_payloads")
  private boolean collectPayloads;
  private final double boost;

  public SpanNear(List<SpanTerm> clauses, int slop, boolean inOrder, boolean collectPayloads, double boost) {
    super();
    this.clauses = clauses;
    this.slop = slop;
    this.inOrder = inOrder;
    this.collectPayloads = collectPayloads;
    this.boost = boost;
    SpanTerm.cleanBoosts(clauses);
  }

}
