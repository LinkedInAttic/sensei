package com.sensei.search.client.json.req.query;

import com.sensei.search.client.json.CustomJsonHandler;
import com.sensei.search.client.json.req.Operator;

/**
 * <p>
 * A family of <code>text</code> queries that accept text, analyzes it, and
 * constructs a query out of it. For example:
 * </p>
 *
 * <pre class="prettyprint lang-js">
 * <span class="pun">{</span><span class="pln"><br>&nbsp; &nbsp; </span><span class="str">"text"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="pun">{</span><span class="pln"><br>&nbsp; &nbsp; &nbsp; &nbsp; </span><span class="str">"message"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="str">"this is a test"</span><span class="pln"><br>&nbsp; &nbsp; </span><span class="pun">}</span><span class="pln"><br></span><span class="pun">}</span>
 * </pre>
 *
 * <p>
 * Note, even though the name is text, it also supports exact matching (
 * <code>term</code> like) on numeric values and dates.
 * </p>
 * <p>
 * Note, <code>message</code> is the name of a field, you can subsitute the name
 * of any field (including <code>_all</code>) instead.
 * </p>
 * <h2>Types of Text Queries</h2> <h3>boolean</h3>
 * <p>
 * The default <code>text</code> query is of type <code>boolean</code>. It means
 * that the text provided is analyzed and the analysis process constructs a
 * boolean query from the provided text. The <code>operator</code> flag can be
 * set to <code>or</code> or <code>and</code> to control the boolean clauses
 * (defaults to <code>or</code>).
 * </p>
 *
 * <p>
 * The <code>analyzer</code> can be set to control which analyzer will perform
 * the analysis process on the text. It default to the field explicit mapping
 * definition, or the default search analyzer.
 * </p>
 * <p>
 * <code>fuzziness</code> can be set to a value (depending on the relevant type,
 * for string types it should be a value between <code>0.0</code> and
 * <code>1.0</code>) to constructs fuzzy queries for each term analyzed. The
 * <code>prefix_length</code> and <code>max_expansions</code> can be set in this
 * case to control the fuzzy process.
 * </p>
 *
 * <p>
 * Here is an example when providing additional parameters (note the slight
 * change in structure, <code>message</code> is the field name):
 * </p>
 *
 * <pre class="prettyprint lang-js">
 * <span class="pun">{</span><span class="pln"><br>&nbsp; &nbsp; </span><span class="str">"text"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="pun">{</span><span class="pln"><br>&nbsp; &nbsp; &nbsp; &nbsp; </span><span class="str">"message"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="pun">{</span><span class="pln"><br>&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; </span><span class="str">"query"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="str">"this is a test"</span><span class="pun">,</span><span class="pln"><br>&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; </span><span class="str">"operator"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="str">"and"</span><span class="pln"><br>&nbsp; &nbsp; &nbsp; &nbsp; </span><span class="pun">}</span><span class="pln"><br>&nbsp; &nbsp; </span><span class="pun">}</span><span class="pln"><br></span><span class="pun">}</span>
 * </pre>
 *
 * <h3>phrase</h3>
 * <p>
 * The <code>text_phrase</code> query analyzes the text and creates a
 * <code>phrase</code> query out of the analyzed text. For example:
 * </p>
 *
 * <pre class="prettyprint lang-js">
 * <span class="pun">{</span><span class="pln"><br>&nbsp; &nbsp; </span><span class="str">"text_phrase"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="pun">{</span><span class="pln"><br>&nbsp; &nbsp; &nbsp; &nbsp; </span><span class="str">"message"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="str">"this is a test"</span><span class="pln"><br>&nbsp; &nbsp; </span><span class="pun">}</span><span class="pln"><br></span><span class="pun">}</span>
 * </pre>
 *
 * <p>
 * Since <code>text_phrase</code> is only a <code>type</code> of a
 * <code>text</code> query, it can also be used in the following manner:
 * </p>
 *
 * <pre class="prettyprint lang-js">
 * <span class="pun">{</span><span class="pln"><br>&nbsp; &nbsp; </span><span class="str">"text"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="pun">{</span><span class="pln"><br>&nbsp; &nbsp; &nbsp; &nbsp; </span><span class="str">"message"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="pun">{</span><span class="pln"><br>&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; </span><span class="str">"query"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="str">"this is a test"</span><span class="pun">,</span><span class="pln"><br>&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; </span><span class="str">"type"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="str">"phrase"</span><span class="pln"><br>&nbsp; &nbsp; &nbsp; &nbsp; </span><span class="pun">}</span><span class="pln"><br>&nbsp; &nbsp; </span><span class="pun">}</span><span class="pln"><br></span><span class="pun">}</span>
 * </pre>
 *
 * <p>
 * A phrase query maintains order of the terms up to a configurable
 * <code>slop</code> (which defaults to 0).
 * </p>
 * <p>
 * The <code>analyzer</code> can be set to control which analyzer will perform
 * the analysis process on the text. It default to the field explicit mapping
 * definition, or the default search analyzer, for example:
 * </p>
 *
 * <pre class="prettyprint lang-js">
 * <span class="pun">{</span><span class="pln"><br>&nbsp; &nbsp; </span><span class="str">"text_phrase"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="pun">{</span><span class="pln"><br>&nbsp; &nbsp; &nbsp; &nbsp; </span><span class="str">"message"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="pun">{</span><span class="pln"><br>&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; </span><span class="str">"query"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="str">"this is a test"</span><span class="pun">,</span><span class="pln"><br>&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; </span><span class="str">"analyzer"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="str">"my_analyzer"</span><span class="pln"><br>&nbsp; &nbsp; &nbsp; &nbsp; </span><span class="pun">}</span><span class="pln"><br>&nbsp; &nbsp; </span><span class="pun">}</span><span class="pln"><br></span><span class="pun">}</span>
 * </pre>
 *
 * <h3>text_phrase_prefix</h3>
 * <p>
 * The <code>text_phrase_prefix</code> is the same as <code>text_phrase</code>,
 * expect it allows for prefix matches on the last term in the text. For
 * example:
 * </p>
 *
 * <pre class="prettyprint lang-js">
 * <span class="pun">{</span><span class="pln"><br>&nbsp; &nbsp; </span><span class="str">"text_phrase_prefix"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="pun">{</span><span class="pln"><br>&nbsp; &nbsp; &nbsp; &nbsp; </span><span class="str">"message"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="str">"this is a test"</span><span class="pln"><br>&nbsp; &nbsp; </span><span class="pun">}</span><span class="pln"><br></span><span class="pun">}</span>
 * </pre>
 *
 * <p>
 * Or:
 * </p>
 *
 * <pre class="prettyprint lang-js">
 * <span class="pun">{</span><span class="pln"><br>&nbsp; &nbsp; </span><span class="str">"text"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="pun">{</span><span class="pln"><br>&nbsp; &nbsp; &nbsp; &nbsp; </span><span class="str">"message"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="pun">{</span><span class="pln"><br>&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; </span><span class="str">"query"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="str">"this is a test"</span><span class="pun">,</span><span class="pln"><br>&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; </span><span class="str">"type"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="str">"phrase_prefix"</span><span class="pln"><br>&nbsp; &nbsp; &nbsp; &nbsp; </span><span class="pun">}</span><span class="pln"><br>&nbsp; &nbsp; </span><span class="pun">}</span><span class="pln"><br></span><span class="pun">}</span>
 * </pre>
 *
 * <p>
 * It accepts the same parameters as the phrase type. In addition, it also
 * accepts a <code>max_expansions</code> parameter that can control to how many
 * prefixes the last term will be expanded. It is highly recommended to set it
 * to an acceptable value to control the execution time of the query. For
 * example:
 * </p>
 *
 * <pre class="prettyprint lang-js">
 * <span class="pun">{</span><span class="pln"><br>&nbsp; &nbsp; </span><span class="str">"text_phrase_prefix"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="pun">{</span><span class="pln"><br>&nbsp; &nbsp; &nbsp; &nbsp; </span><span class="str">"message"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="pun">{</span><span class="pln"><br>&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; </span><span class="str">"query"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="str">"this is a test"</span><span class="pun">,</span><span class="pln"><br>&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; </span><span class="str">"max_expansions"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="lit">10</span><span class="pln"><br>&nbsp; &nbsp; &nbsp; &nbsp; </span><span class="pun">}</span><span class="pln"><br>&nbsp; &nbsp; </span><span class="pun">}</span><span class="pln"><br></span><span class="pun">}</span>
 * </pre>
 *
 * <h2>Comparison to query_string / field</h2>
 * <p>
 * The text family of queries does not go through a “query parsing” process. It
 * does not support field name prefixes, wildcard characters, or other “advance”
 * features. For this reason, chances of it failing are very small / non
 * existent, and it provides an excellent behavior when it comes to just analyze
 * and run that text as a query behavior (which is usually what a text search
 * box does). Also, the <code>phrase_prefix</code> can provide a great “as you
 * type” behavior to automatically load search results.
 * </p>
 *
 *
 */
@CustomJsonHandler(value = QueryJsonHandler.class)
public class TextQuery extends FieldAware implements Query {
  private String value;
  private Operator operator;
  private Type type;
  private double boost;

  public TextQuery(String field, String value, Operator operator, Type type, double boost) {
    super();
    this.field = field;
    this.value = value;
    this.operator = operator;
    this.type = type;
    this.boost = boost;
  }

  public static enum Type {
    phrase_prefix, phrase;
  }

  public String getField() {
    return field;
  }

  public String getValue() {
    return value;
  }

  public Operator getOperator() {
    return operator;
  }

  public Type getType() {
    return type;
  }

  public double getBoost() {
    return boost;
  }

}
