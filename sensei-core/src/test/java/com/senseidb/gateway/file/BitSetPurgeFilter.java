package com.senseidb.gateway.file;

import java.io.IOException;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;

import com.browseengine.bobo.docidset.BitsetDocSet;

public class BitSetPurgeFilter extends Filter {

  @Override
  public DocIdSet getDocIdSet(IndexReader reader) throws IOException {    
    BitsetDocSet bitsetDocSet = new BitsetDocSet();
    bitsetDocSet.addDoc(0);
    return bitsetDocSet;
  }

}
