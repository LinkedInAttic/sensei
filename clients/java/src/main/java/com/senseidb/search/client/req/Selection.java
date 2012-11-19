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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.json.JSONObject;

import com.senseidb.search.client.json.CustomJsonHandler;
import com.senseidb.search.client.req.filter.Filter;
import com.senseidb.search.client.req.query.Query;

@CustomJsonHandler(SelectionJsonHandler.class)
public abstract class Selection extends Query {
    private String field;

    public String getField() {
        return field;
    }
    
    @Override 
    public String toString() {
        return field;
    }

    public Selection setField(String field) {
        this.field = field;
        return this;
    }

    public static class Custom extends Selection {
        private JSONObject custom;

        public Custom(JSONObject custom) {
            super();
            this.custom = custom;
        }

        public Custom() {
            // TODO Auto-generated constructor stub
        }

        public JSONObject getCustom() {
            return custom;
        }
    }

    public static Selection terms(String field, String... values) {
        if (values.length == 1) {
            return new Term(values[0]).setField(field);
        }
        return new Terms(Arrays.asList(values), new ArrayList<String>(), null)
                .setField(field);
    }

    public static Selection terms(String field, List<String> values,
            List<String> excludes, Operator op) {
        return new Terms(values, excludes, op).setField(field);
    }

    public static Selection range(String field, String from, String to,
            boolean includeLower, boolean includeUpper) {
        return new Range(from, to, includeLower, includeUpper).setField(field);
    }

    public static Selection range(String field, String from, String to) {
        return new Range(from, to, true, true).setField(field);
    }

    public static Selection path(String field, String value, boolean strict,
            int depth) {
        return new Path(value, strict, depth).setField(field);
    }

    public static Selection custom(JSONObject custom) {

        return new Custom(custom);
    }
}
