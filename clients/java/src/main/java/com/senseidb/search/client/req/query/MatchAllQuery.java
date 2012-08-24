package com.senseidb.search.client.req.query;

import com.senseidb.search.client.json.CustomJsonHandler;

/**
 *     <p>A query that matches all documents. Maps to Sensei <code>MatchAllDocsQuery</code>.</p>
<pre class="prettyprint lang-js"><span class="pun">{</span><span class="pln"><br>&nbsp; &nbsp; </span><span class="str">"match_all"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="pun">{</span><span class="pln"> </span><span class="pun">}</span><span class="pln"><br></span><span class="pun">}</span></pre>

<p>Which can also have boost associated with it:</p>
<pre class="prettyprint lang-js"><span class="pun">{</span><span class="pln"><br>&nbsp; &nbsp; </span><span class="str">"match_all"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="pun">{</span><span class="pln"> </span><span class="str">"boost"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="lit">1.2</span><span class="pln"> </span><span class="pun">}</span><span class="pln"><br></span><span class="pun">}</span></pre>

<h2>Index Time Boost</h2>
<p>When indexing, a boost value can either be associated on the document level, or per field. The match all query does not take boosting into account by default. In order to take boosting into account, the <code>norms_field</code> needs to be provided in order to explicitly specify which field the boosting will be done on (Note, this will result in slower execution time). For example:</p>
<pre class="prettyprint lang-js"><span class="pun">{</span><span class="pln"><br>&nbsp; &nbsp; </span><span class="str">"match_all"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="pun">{</span><span class="pln"> </span><span class="str">"norms_field"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="str">"my_field"</span><span class="pln"> </span><span class="pun">}</span><span class="pln"><br></span><span class="pun">}</span></pre>


 *
 */
@CustomJsonHandler(QueryJsonHandler.class)
public class MatchAllQuery  extends Query {
    double boost;

    public MatchAllQuery(double boost) {
        super();
        this.boost = boost;
    }
    
}
