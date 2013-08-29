package com.senseidb.facet.docset;

import org.apache.lucene.search.DocIdSet;

import java.io.IOException;

/**
 * @author Dmytro Ivchenko
 */
public abstract class DocSet extends DocIdSet
{
  /**
   * Add a doc id to the set
   * @param docid
   */
  public abstract void addDoc(int docid) throws IOException;

  /**
   * Add an array of sorted docIds to the set
   * @param docids
   * @param start
   * @param len
   */
  public void addDocs(int[] docids, int start, int len) throws IOException
  {
    int i=start;
    while(i<len)
    {
      addDoc(docids[i++]);
    }
  }


  /**
   * Return the set size
   * @return true if present, false otherwise
   */
  public boolean find(int target) throws IOException
  {
    return findWithIndex(target)>-1?true:false;
  }

  /**
   * Return the set size
   * @return index if present, -1 otherwise
   */
  public int findWithIndex(int target) throws IOException
  {
    return -1;
  }

  /**
   * Gets the number of ids in the set
   * @return size of the docset
   */
  public int size() throws IOException
  {
    return 0;
  }

  /**
   * Return the set size in bytes
   * @return index if present, -1 otherwise
   */
  public long sizeInBytes() throws IOException
  {
    return 0;
  }

  /**
   * Optimize by trimming underlying data structures
   */
  public void optimize() throws IOException
  {
    return;
  }


}
