package com.sensei.search.client.json.req.query.span;

import java.util.List;

import com.sensei.search.client.json.CustomJsonHandler;
import com.sensei.search.client.json.JsonField;
import com.sensei.search.client.json.req.query.Query;
import com.sensei.search.client.json.req.query.QueryJsonHandler;
/**
 *     <p>Matches spans which are near one another. One can specify <em>slop</em>, the maximum number of intervening unmatched positions, as well as whether matches are required to be in-order. The span near query maps to Sensei <code>SpanNearQuery</code>. Here is an example:</p>
<pre class="prettyprint lang-js"><span class="pun">{</span><span class="pln"><br>&nbsp; &nbsp; </span><span class="str">"span_near"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="pun">{</span><span class="pln"><br>&nbsp; &nbsp; &nbsp; &nbsp; </span><span class="str">"clauses"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="pun">[</span><span class="pln"><br>&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; </span><span class="pun">{</span><span class="pln"> </span><span class="str">"span_term"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="pun">{</span><span class="pln"> </span><span class="str">"field"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="str">"value1"</span><span class="pln"> </span><span class="pun">}</span><span class="pln"> </span><span class="pun">},</span><span class="pln"><br>&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; </span><span class="pun">{</span><span class="pln"> </span><span class="str">"span_term"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="pun">{</span><span class="pln"> </span><span class="str">"field"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="str">"value2"</span><span class="pln"> </span><span class="pun">}</span><span class="pln"> </span><span class="pun">},</span><span class="pln"><br>&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; </span><span class="pun">{</span><span class="pln"> </span><span class="str">"span_term"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="pun">{</span><span class="pln"> </span><span class="str">"field"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="str">"value3"</span><span class="pln"> </span><span class="pun">}</span><span class="pln"> </span><span class="pun">}</span><span class="pln"><br>&nbsp; &nbsp; &nbsp; &nbsp; </span><span class="pun">],</span><span class="pln"><br>&nbsp; &nbsp; &nbsp; &nbsp; </span><span class="str">"slop"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="lit">12</span><span class="pun">,</span><span class="pln"><br>&nbsp; &nbsp; &nbsp; &nbsp; </span><span class="str">"in_order"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="kwd">false</span><span class="pun">,</span><span class="pln"><br>&nbsp; &nbsp; &nbsp; &nbsp; </span><span class="str">"collect_payloads"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="kwd">false</span><span class="pln"><br>&nbsp; &nbsp; </span><span class="pun">}</span><span class="pln"><br></span><span class="pun">}</span></pre>

<p>The <code>clauses</code> element is a list of one or more other span type queries and the <code>slop</code> controls the maximum number of intervening unmatched positions permitted.</p>

 *
 */
@CustomJsonHandler(QueryJsonHandler.class)
public class SpanNear implements Query  {
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
    }

}
