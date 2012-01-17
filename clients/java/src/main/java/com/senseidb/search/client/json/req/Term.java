package com.senseidb.search.client.json.req;

import com.senseidb.search.client.json.req.query.Query;

/**
     *   <p>Matches documents that have fields that contain a term (<strong>not analyzed</strong>). The term query maps to Sensei <code>TermQuery</code>. The following matches documents where the user field contains the term <code>kimchy</code>:</p>
<pre class="prettyprint lang-js"><span class="pun">{</span><span class="pln"><br>&nbsp; &nbsp; </span><span class="str">"term"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="pun">{</span><span class="pln"> </span><span class="str">"user"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="str">"kimchy"</span><span class="pln"> </span><span class="pun">}</span><span class="pln"><br></span><span class="pun">}</span><span class="pln"> &nbsp; &nbsp;</span></pre>

<p>A boost can also be associated with the query:</p>
<pre class="prettyprint lang-js"><span class="pun">{</span><span class="pln"><br>&nbsp; &nbsp; </span><span class="str">"term"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="pun">{</span><span class="pln"> </span><span class="str">"user"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="pun">{</span><span class="pln"> </span><span class="str">"value"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="str">"kimchy"</span><span class="pun">,</span><span class="pln"> </span><span class="str">"boost"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="lit">2.0</span><span class="pln"> </span><span class="pun">}</span><span class="pln"> </span><span class="pun">}</span><span class="pln"><br></span><span class="pun">}</span><span class="pln"> &nbsp; &nbsp;</span></pre>

<p>Or :</p>
<pre class="prettyprint lang-js"><span class="pun">{</span><span class="pln"><br>&nbsp; &nbsp; </span><span class="str">"term"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="pun">{</span><span class="pln"> </span><span class="str">"user"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="pun">{</span><span class="pln"> </span><span class="str">"term"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="str">"kimchy"</span><span class="pun">,</span><span class="pln"> </span><span class="str">"boost"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="lit">2.0</span><span class="pln"> </span><span class="pun">}</span><span class="pln"> </span><span class="pun">}</span><span class="pln"><br></span><span class="pun">}</span><span class="pln"> &nbsp; &nbsp;</span></pre>


     *
     */
    public class Term extends Selection implements Query {
        private String value;
        private double boost;
        public Term(String value) {
            super();
            this.value = value;
        }
        public Term(String value, double boost) {
          super();
          this.value = value;
          this.boost = boost;
      }
      public Term(){
    }
    }