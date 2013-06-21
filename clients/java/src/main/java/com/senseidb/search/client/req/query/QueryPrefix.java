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

package com.senseidb.search.client.req.query;

import com.senseidb.search.client.json.CustomJsonHandler;

/**
 * <p>
 * Matches documents that have fields containing terms with a specified prefix
 * (<strong>not analyzed</strong>). The prefix query maps to Sensei
 * <code>PrefixQuery</code>. The following matches documents where the user
 * field contains a term that starts with <code>ki</code>:
 * </p>
 * 
 * <pre class="prettyprint lang-js">
 * <span class="pun">{</span><span class="pln"><br>&nbsp; &nbsp; </span><span class="str">"prefix"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="pun">{</span><span class="pln"> </span><span class="str">"user"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="str">"ki"</span><span class="pln"> </span><span class="pun">}</span><span class="pln"><br></span><span class="pun">}</span>
 * </pre>
 * 
 * <p>
 * A boost can also be associated with the query:
 * </p>
 * 
 * <p>
 * Or :
 * </p>
 * 
 * 
 * <p>
 * This multi term query allows to control how it gets rewritten using the <a
 * href="multi-term-rewrite.html">rewrite</a> parameter.
 * </p>
 * 
 * 
 */
@CustomJsonHandler(QueryJsonHandler.class)
public class QueryPrefix extends FieldAwareQuery {
    private String value;
    private double boost;

    public QueryPrefix(String field, String value, double boost) {
        super();
        this.value = value;
        this.boost = boost;
        this.field = field;
    }

    public String getValue() {
        return value;
    }

    public double getBoost() {
        return boost;
    }

}
