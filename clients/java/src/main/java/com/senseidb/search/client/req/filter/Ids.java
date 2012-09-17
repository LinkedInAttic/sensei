/**
 * This software is licensed to you under the Apache License, Version 2.0 (the
 * "Apache License").
 *
 * LinkedIn's contributions are made under the Apache License. If you contribute
 * to the Software, the contributions will be deemed to have been made under the
 * Apache License, unless you expressly indicate otherwise. Please do not make any
 * contributions that would be inconsistent with the Apache License.
 *
 * You may obtain a copy of the Apache License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, this software
 * distributed under the Apache License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the Apache
 * License for the specific language governing permissions and limitations for the
 * software governed under the Apache License.
 *
 * © 2012 LinkedIn Corp. All Rights Reserved.  
 */

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
