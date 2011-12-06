package com.sensei.search.client.json.req.query.span;

import com.sensei.search.client.json.CustomJsonHandler;
import com.sensei.search.client.json.req.query.Query;
import com.sensei.search.client.json.req.query.QueryJsonHandler;
/**
 *     <p>Removes matches which overlap with another span query. The span not query maps to Sensei <code>SpanNotQuery</code>. Here is an example:</p>
<pre class="prettyprint lang-js"><span class="pun">{</span><span class="pln"><br>&nbsp; &nbsp; </span><span class="str">"span_not"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="pun">{</span><span class="pln"><br>&nbsp; &nbsp; &nbsp; &nbsp; </span><span class="pun">©</span><span class="str">"include"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="pun">{</span><span class="pln"><br>&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; </span><span class="str">"span_term"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="pun">{</span><span class="pln"> </span><span class="str">"field1"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="str">"value1"</span><span class="pln"> </span><span class="pun">}</span><span class="pln"><br>&nbsp; &nbsp; &nbsp; &nbsp; </span><span class="pun">},</span><span class="pln"><br>&nbsp; &nbsp; &nbsp; &nbsp; </span><span class="str">"exclude"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="pun">{</span><span class="pln"><br>&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; </span><span class="str">"span_term"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="pun">{</span><span class="pln"> </span><span class="str">"field2"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="str">"value2"</span><span class="pln"> </span><span class="pun">}</span><span class="pln"><br>&nbsp; &nbsp; &nbsp; &nbsp; </span><span class="pun">}</span><span class="pln"><br>&nbsp; &nbsp; </span><span class="pun">}</span><span class="pln"><br></span><span class="pun">}</span></pre>

<p>The <code>include</code> and <code>exclude</code> clauses can be any span type query. The <code>include</code> clause is the span query whose matches are filtered, and the <code>exclude</code> clause is the span query whose matches must not overlap those returned.</p>

 *
 */
@CustomJsonHandler(QueryJsonHandler.class)
public class SpanNot implements Query  {

    SpanTerm include;

    SpanTerm exclude;

    private final double boost;

    public SpanNot(SpanTerm include, SpanTerm exclude, double boost) {
        super();
        this.include = include;
        include.setBoost(null);
        exclude.setBoost(null);
        this.exclude = exclude;
        this.boost = boost;
    }

}
