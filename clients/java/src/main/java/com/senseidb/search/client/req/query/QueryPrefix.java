package com.senseidb.search.client.req.query;

import com.senseidb.search.client.json.CustomJsonHandler;

/**
 * <p>
 * Matches documents that have fields containing terms with a specified prefix
 * (<strong>not analyzed</strong>). The prefix query maps to Sensei
 * <code>PrefixQuery</code>. The following matches documents where the user
 * field contains a term that starts with <code>ki</code>:
 * </p>
 *
 * <pre class="prettyprint lang-js">
 * <span class="pun">{</span><span class="pln"><br>&nbsp; &nbsp; </span><span class="str">"prefix"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="pun">{</span><span class="pln"> </span><span class="str">"user"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="str">"ki"</span><span class="pln"> </span><span class="pun">}</span><span class="pln"><br></span><span class="pun">}</span>
 * </pre>
 *
 * <p>
 * A boost can also be associated with the query:
 * </p>
 *
 * <p>
 * Or :
 * </p>
 *
 *
 * <p>
 * This multi term query allows to control how it gets rewritten using the <a
 * href="multi-term-rewrite.html">rewrite</a> parameter.
 * </p>
 *
 *
 */
@CustomJsonHandler(QueryJsonHandler.class)
public class QueryPrefix extends FieldAwareQuery {
  private String value;
  private double boost;

  public QueryPrefix(String field, String value, double boost) {
    super();
    this.value = value;
    this.boost = boost;
    this.field = field;
  }

}
