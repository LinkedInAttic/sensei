package com.sensei.search.client.json.req;

import java.util.List;

import com.sensei.search.client.json.JsonField;
import com.sensei.search.client.json.req.query.Query;

/**
     * <p>A query that match on any (configurable) of the provided terms. This is a simpler syntax query for using a <code>bool</code> query with several <code>term</code> queries in the <code>should</code> clauses. For example:</p>
<pre class="prettyprint lang-js"><span class="pun">{</span><span class="pln"><br>&nbsp; &nbsp; </span><span class="str">"terms"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="pun">{</span><span class="pln"><br>&nbsp; &nbsp; &nbsp; &nbsp; </span><span class="str">"tags"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="pun">[</span><span class="pln"> </span><span class="str">"blue"</span><span class="pun">,</span><span class="pln"> </span><span class="str">"pill"</span><span class="pln"> </span><span class="pun">],</span><span class="pln"><br>&nbsp; &nbsp; &nbsp; &nbsp; </span><span class="str">"minimum_match"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="lit">1</span><span class="pln"><br>&nbsp; &nbsp; </span><span class="pun">}</span><span class="pln"><br></span><span class="pun">}</span></pre>

<p>The <code>terms</code> query is also aliased with <code>in</code> as the query name for simpler usage.</p>

     *
     */
    public class Terms extends Selection implements Query {

        List<String> values;
        List<String> excludes;
        Operator op;
         Double boost;
         @JsonField("minimum_match")
         Integer minimumMatch;
        public Terms() {

        }
        public Terms( List<String> values, List<String> excludes, Operator op) {
            super();
            this.values = values;
            this.excludes = excludes;
            this.op = op;
        }
        public Terms(List<String> values, List<String> excludes, Operator op, int minimumMatch, double boost) {
          super();
          this.values = values;
          this.excludes = excludes;
          this.op = op;
          this.boost = boost;
          this.minimumMatch = minimumMatch;
        }

    }