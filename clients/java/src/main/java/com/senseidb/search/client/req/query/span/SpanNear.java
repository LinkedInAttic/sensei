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

import java.util.List;

import com.senseidb.search.client.json.CustomJsonHandler;
import com.senseidb.search.client.json.JsonField;
import com.senseidb.search.client.req.query.Query;
import com.senseidb.search.client.req.query.QueryJsonHandler;

/**
 * <p>
 * Matches spans which are near one another. One can specify <em>slop</em>, the
 * maximum number of intervening unmatched positions, as well as whether matches
 * are required to be in-order. The span near query maps to Sensei
 * <code>SpanNearQuery</code>. Here is an example:
 * </p>
 * 
 * 
 * <p>
 * The <code>clauses</code> element is a list of one or more other span type
 * queries and the <code>slop</code> controls the maximum number of intervening
 * unmatched positions permitted.
 * </p>
 * 
 * 
 */
@CustomJsonHandler(QueryJsonHandler.class)
public class SpanNear extends Query {
    private List<SpanTerm> clauses;
    private int slop;
    @JsonField("in_order")
    private boolean inOrder;
    @JsonField("collect_payloads")
    private boolean collectPayloads;
    private final double boost;

    public SpanNear(List<SpanTerm> clauses, int slop, boolean inOrder,
            boolean collectPayloads, double boost) {
        super();
        this.clauses = clauses;
        this.slop = slop;
        this.inOrder = inOrder;
        this.collectPayloads = collectPayloads;
        this.boost = boost;
        SpanTerm.cleanBoosts(clauses);
    }
    
    public List<SpanTerm> getClauses() {
        return clauses;
    }

    public int getSlop() {
        return slop;
    }

    public boolean isInOrder() {
        return inOrder;
    }

    public boolean isCollectPayloads() {
        return collectPayloads;
    }

    public double getBoost() {
        return boost;
    }

}
