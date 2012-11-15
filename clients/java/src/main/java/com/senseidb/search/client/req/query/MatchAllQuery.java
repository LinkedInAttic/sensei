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
 * A query that matches all documents. Maps to Sensei
 * <code>MatchAllDocsQuery</code>.
 * </p>
 * 
 * <pre class="prettyprint lang-js">
 * <span class="pun">{</span><span class="pln"><br>&nbsp; &nbsp; </span><span class="str">"match_all"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="pun">{</span><span class="pln"> </span><span class="pun">}</span><span class="pln"><br></span><span class="pun">}</span>
 * </pre>
 * 
 * <p>
 * Which can also have boost associated with it:
 * </p>
 * 
 * <pre class="prettyprint lang-js">
 * <span class="pun">{</span><span class="pln"><br>&nbsp; &nbsp; </span><span class="str">"match_all"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="pun">{</span><span class="pln"> </span><span class="str">"boost"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="lit">1.2</span><span class="pln"> </span><span class="pun">}</span><span class="pln"><br></span><span class="pun">}</span>
 * </pre>
 * 
 * <h2>Index Time Boost</h2>
 * <p>
 * When indexing, a boost value can either be associated on the document level,
 * or per field. The match all query does not take boosting into account by
 * default. In order to take boosting into account, the <code>norms_field</code>
 * needs to be provided in order to explicitly specify which field the boosting
 * will be done on (Note, this will result in slower execution time). For
 * example:
 * </p>
 * 
 * <pre class="prettyprint lang-js">
 * <span class="pun">{</span><span class="pln"><br>&nbsp; &nbsp; </span><span class="str">"match_all"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="pun">{</span><span class="pln"> </span><span class="str">"norms_field"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="str">"my_field"</span><span class="pln"> </span><span class="pun">}</span><span class="pln"><br></span><span class="pun">}</span>
 * </pre>
 * 
 * 
 * 
 */
@CustomJsonHandler(QueryJsonHandler.class)
public class MatchAllQuery extends Query {
    double boost;

    public MatchAllQuery(double boost) {
        super();
        this.boost = boost;
    }

}
