package com.sensei.search.res.json.domain;

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
   
}
