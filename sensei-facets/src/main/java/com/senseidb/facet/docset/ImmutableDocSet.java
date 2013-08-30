package com.senseidb.facet.docset;

import org.apache.lucene.search.DocIdSetIterator;

import java.io.IOException;

/**
 * @author Dmytro Ivchenko
 */
public abstract class ImmutableDocSet extends DocSet
{
  private int size = -1;

  @Override
  public void addDoc(int docid)
  {
    throw new java.lang.UnsupportedOperationException("Attempt to add document to an immutable data structure");

  }

  @Override
  public int size() throws IOException
  {
    // Do the size if we haven't done it so far.
    if(size < 0)
    {
      DocIdSetIterator dcit = this.iterator();
      size = 0;
      try {
        while(dcit.nextDoc() != DocIdSetIterator.NO_MORE_DOCS)
          size++;
      } catch (IOException e) {
        return -1;
      }
    }
    return size;
  }

}
