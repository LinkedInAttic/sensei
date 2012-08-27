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

package com.senseidb.search.client.req.relevance;

/**
 * Supported variable types:
 * <ul>
 * <li>Variable type:
 * <li>HashSet: in detail, set_int, set_float, set_string, set_double,
 * set_long|set_int, set_float, set_string, set_double, set_long.
 * <li>HashMap: e.g., map_int_int, map_int_double, map_int_float,
 * map_string_int, map_string_double, etc. (Currently support two types hashmap:
 * map_int_* and map_string_*)
 * <li>other normal type: int, double, float, long, bool, string.
 * </ul>
 * 
 * 
 */
public enum VariableType {
  set_int, set_float, set_string, set_double, set_long, map_int_int, map_int_double, map_int_float, map_int_long, map_int_bool, map_int_string, map_string_int, map_string_double, map_string_float, map_string_long, map_string_bool, map_string_string, type_int, type_double, type_float, type_long, type_bool, type_string;
  public String getValue() {
    if (this.name().startsWith("type_")) {
      return this.name().substring("type_".length());
    }
    return this.name();
  }
}
