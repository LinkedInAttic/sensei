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
import com.senseidb.search.client.req.query.FieldAwareQuery;
import com.senseidb.search.client.req.query.QueryJsonHandler;

/**
 * <p>
 * Matches spans containing a term. The span term query maps to Sensei
 * <code>SpanTermQuery</code>. Here is an example:
 */
@CustomJsonHandler(QueryJsonHandler.class)
public class SpanTerm extends FieldAwareQuery {
  private String value;
  private Double boost;

  public SpanTerm(String field, String value, Double boost) {
    super();
    this.value = value;
    this.boost = boost;
    this.field = field;
  }

  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value;
  }

  public Double getBoost() {
    return boost;
  }

  public void setBoost(Double boost) {
    this.boost = boost;
  }

  public static void cleanBoosts(List<SpanTerm> spanTerms) {
    for (SpanTerm spanTerm : spanTerms) {
      spanTerm.setBoost(null);
    }
  }

}
