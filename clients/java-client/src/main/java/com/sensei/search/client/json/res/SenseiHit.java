package com.sensei.search.client.json.res;

import java.util.ArrayList;
import java.util.List;

public class SenseiHit {

   private Integer uid;
   private Integer docid;
   private Integer score;
   private String srcdata;
   private Integer grouphitscount;
   private List<SenseiHit> groupHits = new ArrayList<SenseiHit>();
@Override
public String toString() {
    return "SenseiHit [uid=" + uid + ", docid=" + docid + ", score=" + score + ", srcdata=" + srcdata
            + ", grouphitscount=" + grouphitscount + ", groupHits=" + groupHits + "]";
}
public Integer getUid() {
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

}
