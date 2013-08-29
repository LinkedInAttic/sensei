package com.senseidb.facet.docset;

import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;

import java.io.IOException;
import java.io.Serializable;

/**
 * @author Dmytro Ivchenko
 */
public class NotDocIdSet extends ImmutableDocSet implements Serializable {

  private static final long serialVersionUID = 1L;

  private DocIdSet innerSet = null;

  private int max = -1;

  public NotDocIdSet(DocIdSet docSet, int maxVal) {
    innerSet = docSet;
    max = maxVal;
  }

  class NotDocIdSetIterator extends DocIdSetIterator {
    int lastReturn = -1;
    private DocIdSetIterator it1 = null;
    private int innerDocid = -1;

    NotDocIdSetIterator() throws IOException {
      initialize();
    }

    private void initialize() throws IOException{
      it1 = innerSet.iterator();

      try {
        if ((innerDocid = it1.nextDoc()) == DocIdSetIterator.NO_MORE_DOCS) it1 = null;
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    @Override
    public int docID() {
      return lastReturn;
    }

    @Override
    public int nextDoc() throws IOException {
      return advance(0);
    }

    @Override
    public int advance(int target) throws IOException {

      if (lastReturn == DocIdSetIterator.NO_MORE_DOCS) {
        return DocIdSetIterator.NO_MORE_DOCS;
      }

      if (target <= lastReturn) target = lastReturn + 1;

      if (target >= max) {
        return (lastReturn =  DocIdSetIterator.NO_MORE_DOCS);
      }

      if (it1 != null && innerDocid < target) {
        if ((innerDocid = it1.advance(target)) == DocIdSetIterator.NO_MORE_DOCS) {
          it1 = null;
        }
      }

      while (it1 != null && innerDocid == target) {
        target++;
        if (target >= max) {
          return (lastReturn =  DocIdSetIterator.NO_MORE_DOCS);
        }
        if ((innerDocid = it1.advance(target)) == DocIdSetIterator.NO_MORE_DOCS) {
          it1 = null;
        }
      }
      return (lastReturn = target);
    }

    @Override
    public long cost() {
      return it1.cost();
    }
  }

  @Override
  public DocIdSetIterator iterator() throws IOException{
    return new NotDocIdSetIterator();
  }

  /**
   * Find existence in the set with index
   *
   * NOTE :  Expensive call. Avoid.
   * @param val value to find the index for
   * @return index if the given value
   */
  @Override
  public int findWithIndex(int val) throws IOException
  {
    DocIdSetIterator finder = new NotDocIdSetIterator();
    int cursor = -1;
    try {
      int docid;
      while((docid = finder.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS)
      {
        if(docid > val)
          return -1;
        else if(docid == val )
          return ++cursor;
        else
          ++cursor;


      }
    } catch (IOException e) {
      return -1;
    }
    return -1;
  }


}
