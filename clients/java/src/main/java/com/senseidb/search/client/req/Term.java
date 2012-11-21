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

package com.senseidb.search.client.req;

import com.senseidb.search.client.req.query.Query;

/**
 * <p>
 * Matches documents that have fields that contain a term (<strong>not
 * analyzed</strong>). The term query maps to Sensei <code>TermQuery</code>. The
 * following matches documents where the user field contains the term
 * <code>kimchy</code>:
 * </p>
 * 
 */
public class Term extends Selection {
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

    public Term() {
    }

    public String getValue() {
        return value;
    }

    public double getBoost() {
        return boost;
    }
    
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(super.toString()); // for field
        builder.append(value == null ? "" : value);
        builder.append("^").append(boost);
        return builder.toString();
    }
}
