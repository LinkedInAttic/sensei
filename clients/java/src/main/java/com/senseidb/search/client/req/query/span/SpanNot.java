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
 * Removes matches which overlap with another span query. The span not query
 * maps to Sensei <code>SpanNotQuery</code>. Here is an example:
 * </p>
 *
 *
 * <p>
 * The <code>include</code> and <code>exclude</code> clauses can be any span
 * type query. The <code>include</code> clause is the span query whose matches
 * are filtered, and the <code>exclude</code> clause is the span query whose
 * matches must not overlap those returned.
 * </p>
 *
 *
 */
@CustomJsonHandler(QueryJsonHandler.class)
public class SpanNot extends Query {

  SpanTerm include;

  SpanTerm exclude;

  private final double boost;

  public SpanNot(SpanTerm include, SpanTerm exclude, double boost) {
    super();
    this.include = include;
    include.setBoost(null);
    exclude.setBoost(null);
    this.exclude = exclude;
    this.boost = boost;
  }

}
