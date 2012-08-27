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

package com.senseidb.search.client.res;

import com.senseidb.search.client.json.JsonField;

public class TermFrequency {
  private String term;
  @JsonField("freq")
  private Integer frequency;
  public String getTerm() {
    return term;
  }
  public void setTerm(String term) {
    this.term = term;
  }
  public int getFrequency() {
    return frequency;
  }
  public void setFrequency(int frequency) {
    this.frequency = frequency;
  }
  public TermFrequency(String term, int frequency) {
    super();
    this.term = term;
    this.frequency = frequency;
  }
public TermFrequency() {
  // TODO Auto-generated constructor stub
}
@Override
public String toString() {
  return "TermFrequency [term=" + term + ", frequency=" + frequency + "]";
}

}
