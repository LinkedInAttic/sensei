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

package com.senseidb.search.client.req.query.span;

import com.senseidb.search.client.json.CustomJsonHandler;
import com.senseidb.search.client.req.query.Query;
import com.senseidb.search.client.req.query.QueryJsonHandler;

/**
 * <p>
 * Matches spans near the beginning of a field. The span first query maps to
 * Sensei <code>SpanFirstQuery</code>. Here is an example:
 * </p>
 * 
 * 
 * <p>
 * The <code>match</code> clause can be any other span type query. The
 * <code>end</code> controls the maximum end position permitted in a match.
 * </p>
 * 
 * 
 */
@CustomJsonHandler(QueryJsonHandler.class)
public class SpanFirst extends Query {

    SpanTerm match;
    int end;
    private double boost;

    public SpanFirst(SpanTerm match, int end, double boost) {
        super();
        this.match = match;
        this.end = end;
        this.boost = boost;
        match.setBoost(null);
    }

}
