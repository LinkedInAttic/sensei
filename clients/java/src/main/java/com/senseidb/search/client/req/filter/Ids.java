package com.senseidb.search.client.req.filter;

import java.util.List;

import com.senseidb.search.client.json.CustomJsonHandler;
import com.senseidb.search.client.req.query.Query;
import com.senseidb.search.client.req.query.QueryJsonHandler;

/**
 *  <p>Filters documents that only have the provided ids. Note, this filter does not require the <code>_id</code> field to be indexed since it works using the <code>_uid</code> field.</p>
<pre class="prettyprint lang-js"><span class="pun">{</span><span class="pln"><br>&nbsp; &nbsp; </span><span class="str">"ids"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="pun">{</span><span class="pln"><br>&nbsp; &nbsp; &nbsp; &nbsp; </span><span class="str">"type"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="str">"my_type"</span><span class="pln"><br>&nbsp; &nbsp; &nbsp; &nbsp; </span><span class="str">"values"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="pun">[</span><span class="str">"1"</span><span class="pun">,</span><span class="pln"> </span><span class="str">"4"</span><span class="pun">,</span><span class="pln"> </span><span class="str">"100"</span><span class="pun">]</span><span class="pln"><br>&nbsp; &nbsp; </span><span class="pun">}</span><span class="pln"><br></span><span class="pun">}</span><span class="pln"> &nbsp; &nbsp;</span></pre>

<p>The <code>type</code> is optional and can be omitted, and can also accept an array of values.</p>

 *
 */
@CustomJsonHandler(QueryJsonHandler.class)
public class Ids extends Query implements Filter {
    List<String> values;
    List<String> excludes;
    private double boost;
    public Ids(List<String> values, List<String> excludes) {
        super();
        this.values = values;
        this.excludes = excludes;
    }
    public Ids(List<String> values, List<String> excludes, double boost) {
      super();
      this.values = values;
      this.excludes = excludes;
      this.boost = boost;
  }
    public List<String> getValues() {
        return values;
    }
    public List<String> getExcludes() {
        return excludes;
    }

}