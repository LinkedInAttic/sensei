package com.sensei.search.client.json.res;

import com.sensei.search.client.json.JsonField;

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
