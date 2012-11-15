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

/**
 * This parameter specifies how the search results should be sorted. The results
 * can be sorted based on one or multiple fields, in either ascending or
 * descending order. The value of this parameter consists of a list of comma
 * separated strings, each of which can be one of the following values: <br>
 * • relevance: this means that the results should be sorted by scores in
 * descending order. <br>
 * • relrev: this means that the results should be sorted by scores in ascending
 * order. <br>
 * • doc: this means that the results should be sorted by doc ids in ascending
 * order. <br>
 * • docrev: this means that the results should be sorted by doc ids in
 * descending order. <br>
 * • <field-name>:<direction>: this means that the results should be sorted by
 * field <field-name> in the direction of <direction>, which can be either asc
 * or desc. Example : Sort Fields Parameters <br>
 * sort=relevance <br>
 * sort=docrev <br>
 * sort=price:desc,color=asc
 * 
 * 
 */
public class Sort {
    private String field;
    private String order;

    public static Sort asc(String field) {
        Sort sort = new Sort();
        sort.field = field;
        sort.order = Order.asc.name();
        return sort;
    }

    public static Sort desc(String field) {
        Sort sort = new Sort();
        sort.field = field;
        sort.order = Order.desc.name();
        return sort;
    }

    public static Sort byRelevance() {
        Sort sort = new Sort();
        sort.field = "_score";
        return sort;
    }

    public static enum Order {
        desc, asc;
    }

    public String getField() {
        return field;
    }

    public Order getOrder() {
        if (order == null) {
            return null;
        }
        return Order.valueOf(order);
    }

}
