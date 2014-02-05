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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.senseidb.search.client.json.CustomJsonHandler;
import com.senseidb.search.client.json.JsonField;
@CustomJsonHandler(SenseiHitJsonHandler.class)
public class SenseiHit {
  @JsonField("_uid")
  private Long uid;
  @JsonField("_docid")
  private Integer docid;
  @JsonField("_score")
  private Double score;
  @JsonField("_srcdata")
  private String srcdata;
  @JsonField("_grouphitscount")
  private Integer grouphitscount;
  private List<Float> features;
  private List<SenseiHit> groupHits = new ArrayList<SenseiHit>();
  private List<FieldValue> storedFields = new ArrayList<FieldValue>();
  @JsonField("termvectors")
  private Map<String, List<TermFrequency>> fieldTermFrequencies = new HashMap<String, List<TermFrequency>>();
  private Explanation explanation;
  private Map<String, List<String>> fieldValues = new HashMap<String, List<String>>();

  @Override
  public String toString() {
    return "\n---------------------------------------------------------------------------------------------------------------\n" +
    		"SenseiHit [uid=" + uid + ", docid=" + docid + ", score=" + score + ", srcdata=" + srcdata
        + ", grouphitscount=" + grouphitscount + ", \n      groupHits=" + groupHits + ", \n     storedFields=" + storedFields
        + ", \n     fieldTermFrequencies=" + fieldTermFrequencies + ", \n      explanation=" + explanation + ", \n       fieldValues="
        + fieldValues + ", \n   features=" + features + "]";
  }



  public Long getUid() {
    return uid;
  }

  public Integer getDocid() {
    return docid;
  }

  public Double getScore() {
    return score;
  }

  public String getSrcdata() {
    return srcdata;
  }

  public Integer getGrouphitscount() {
    return grouphitscount;
  }

  public List<SenseiHit> getGroupHits() {
    return groupHits;
  }

  public void setUid(Long uid) {
    this.uid = uid;
  }

  public void setDocid(Integer docid) {
    this.docid = docid;
  }

  public void setScore(Double score) {
    this.score = score;
  }

  public void setSrcdata(String srcdata) {
    this.srcdata = srcdata;
  }

  public void setGrouphitscount(Integer grouphitscount) {
    this.grouphitscount = grouphitscount;
  }

  public void setGroupHits(List<SenseiHit> groupHits) {
    this.groupHits = groupHits;
  }

  public List<FieldValue> getStoredFields() {
    return storedFields;
  }

  public void setStoredFields(List<FieldValue> storedFields) {
    this.storedFields = storedFields;
  }

  public Map<String, List<TermFrequency>> getFieldTermFrequencies() {
    return fieldTermFrequencies;
  }

  public Explanation getExplanation() {
    return explanation;
  }

  public Map<String, List<String>> getFieldValues() {
    return fieldValues;
  }

  public List<Float> getFeatures() {
    return features;
  }

  public void setFeatures(List<Float> features) {
    this.features = features;
  }

}
