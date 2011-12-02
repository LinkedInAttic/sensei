package com.sensei.search.client.json.req.query.span;

import com.sensei.search.client.json.CustomJsonHandler;
import com.sensei.search.client.json.req.query.Query;
import com.sensei.search.client.json.req.query.QueryJsonHandler;
/**
 * <p>Matches spans near the beginning of a field. The span first query maps to Sensei <code>SpanFirstQuery</code>. Here is an example:</p>
<pre class="prettyprint lang-js"><span class="pun">{</span><span class="pln"><br>&nbsp; &nbsp; </span><span class="str">"span_first"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="pun">{</span><span class="pln"><br>&nbsp; &nbsp; &nbsp; &nbsp; </span><span class="str">"match"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="pun">{</span><span class="pln"><br>&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; </span><span class="str">"span_term"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="pun">{</span><span class="pln"> </span><span class="str">"user"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="str">"kimchy"</span><span class="pln"> </span><span class="pun">}</span><span class="pln"><br>&nbsp; &nbsp; &nbsp; &nbsp; </span><span class="pun">},</span><span class="pln"><br>&nbsp; &nbsp; &nbsp; &nbsp; </span><span class="str">"end"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="lit">3</span><span class="pln"><br>&nbsp; &nbsp; </span><span class="pun">}</span><span class="pln"><br></span><span class="pun">}</span><span class="pln"> &nbsp; &nbsp;</span></pre>

<p>The <code>match</code> clause can be any other span type query. The <code>end</code> controls the maximum end position permitted in a match.</p>

 *
 */
@CustomJsonHandler(QueryJsonHandler.class)
public class SpanFirst implements Query {
    
    SpanTerm match;
    int end;
    public SpanFirst(SpanTerm match, int end) {
        super();
        this.match = match;
        this.end = end;
    }
    
    
}
