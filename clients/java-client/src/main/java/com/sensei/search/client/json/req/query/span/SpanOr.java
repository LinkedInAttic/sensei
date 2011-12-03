package com.sensei.search.client.json.req.query.span;

import java.util.List;

import com.sensei.search.client.json.CustomJsonHandler;
import com.sensei.search.client.json.req.query.Query;
import com.sensei.search.client.json.req.query.QueryJsonHandler;
/**
 *  <p>Matches the union of its span clauses. The span or query maps to Sensei <code>SpanOrQuery</code>. Here is an example:</p>
<pre class="prettyprint lang-js"><span class="pun">{</span><span class="pln"><br>&nbsp; &nbsp; </span><span class="str">"span_or"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="pun">{</span><span class="pln"><br>&nbsp; &nbsp; &nbsp; &nbsp; </span><span class="str">"clauses"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="pun">[</span><span class="pln"><br>&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; </span><span class="pun">{</span><span class="pln"> </span><span class="str">"span_term"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="pun">{</span><span class="pln"> </span><span class="str">"field"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="str">"value1"</span><span class="pln"> </span><span class="pun">}</span><span class="pln"> </span><span class="pun">},</span><span class="pln"><br>&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; </span><span class="pun">{</span><span class="pln"> </span><span class="str">"span_term"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="pun">{</span><span class="pln"> </span><span class="str">"field"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="str">"value2"</span><span class="pln"> </span><span class="pun">}</span><span class="pln"> </span><span class="pun">},</span><span class="pln"><br>&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; </span><span class="pun">{</span><span class="pln"> </span><span class="str">"span_term"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="pun">{</span><span class="pln"> </span><span class="str">"field"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="str">"value3"</span><span class="pln"> </span><span class="pun">}</span><span class="pln"> </span><span class="pun">}</span><span class="pln"><br>&nbsp; &nbsp; &nbsp; &nbsp; </span><span class="pun">]</span><span class="pln"><br>&nbsp; &nbsp; </span><span class="pun">}</span><span class="pln"><br></span><span class="pun">}</span></pre>

<p>The <code>clauses</code> element is a list of one or more other span type queries.</p>

 *
 */
@CustomJsonHandler(QueryJsonHandler.class)
public class SpanOr implements Query  {
    List<SpanTerm> clauses;

    public SpanOr(List<SpanTerm> clauses) {
        super();
        this.clauses = clauses;
    }
    
}
