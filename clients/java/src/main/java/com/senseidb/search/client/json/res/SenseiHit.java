package com.senseidb.search.client.json.res;

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
  private Integer score;
  @JsonField("_srcdata")
  private String srcdata;
  @JsonField("_grouphitscount")
  private Integer grouphitscount;
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
        + fieldValues + "]";
  }



  public Long getUid() {
    return uid;
  }

  public Integer getDocid() {
    return docid;
  }

  public Integer getScore() {
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

  public void setScore(Integer score) {
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

}
