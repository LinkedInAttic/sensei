package com.senseidb.indexing.activity.deletion;

import org.apache.lucene.index.IndexReader;

/**
 * Provides the callback method which will be called if the some documents are deleted from zoie
 *
 */
public interface DeletionListener {
  public void onDelete(IndexReader indexReader, long... uids);
  
}
