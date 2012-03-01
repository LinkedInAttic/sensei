package com.senseidb.indexing.activity.deletion;

import org.apache.lucene.index.IndexReader;

public interface DeletionListener {
  public void onDelete(long uid, IndexReader indexReader);
  
}
