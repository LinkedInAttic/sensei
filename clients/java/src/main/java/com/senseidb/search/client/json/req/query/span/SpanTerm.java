package com.senseidb.search.client.json.req.query.span;

import java.util.List;

import com.senseidb.search.client.json.CustomJsonHandler;
import com.senseidb.search.client.json.req.query.FieldAware;
import com.senseidb.search.client.json.req.query.Query;
import com.senseidb.search.client.json.req.query.QueryJsonHandler;

/**
 * <p>
 * Matches spans containing a term. The span term query maps to Sensei
 * <code>SpanTermQuery</code>. Here is an example:
 * </p>
 *
 * <pre class="prettyprint lang-js">
 * <span class="pun">{</span><span class="pln"><br>&nbsp; &nbsp; </span><span class="str">"span_term"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="pun">{</span><span class="pln"> </span><span class="str">"user"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="str">"kimchy"</span><span class="pln"> </span><span class="pun">}</span><span class="pln"><br></span><span class="pun">}</span><span class="pln"> &nbsp; &nbsp;</span>
 * </pre>
 *
 * <p>
 * A boost can also be associated with the query:
 * </p>
 *
 *
 * <p>
 * Or :
 * </p>
 *
 *
 *
 *
 */
@CustomJsonHandler(QueryJsonHandler.class)
public class SpanTerm extends FieldAware implements Query {
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
