package com.senseidb.facet.docset;

import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;

import java.io.IOException;
import java.io.Serializable;
import java.util.Comparator;
import java.util.List;

/**
 * @author Dmytro Ivchenko
 */
public class OrDocIdSet extends ImmutableDocSet implements Serializable {
  private static final long serialVersionUID = 1L;

  private static final int INVALID = -1;


  public class AescDocIdSetComparator implements Comparator<DocIdSetIterator>,
      Serializable {

    private static final long serialVersionUID = 1L;

    public int compare(DocIdSetIterator o1, DocIdSetIterator o2) {
      return o1.docID() - o2.docID();
    }

  }

  List<DocIdSet> sets = null;

  private int _size = INVALID;

  public OrDocIdSet(List<DocIdSet> docSets) {
    this.sets = docSets;
    int size = 0;
    if (sets != null) {
      for(DocIdSet set : sets) {
        if(set != null) size++;
      }
    }
  }

  @Override
  public DocIdSetIterator iterator() throws IOException {
    return new OrDocIdSetIterator(sets);
  }


  /**
   * Find existence in the set with index
   *
   * NOTE :  Expensive call. Avoid.
   * @param val value to find the index for
   * @return index where the value is
   */
  @Override
  public int findWithIndex(int val) throws IOException
  {
    DocIdSetIterator finder = new OrDocIdSetIterator(sets);
    int cursor = -1;
    try {
      int docid;
      while((docid = finder.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS)
      {
        if(docid > val)
          return -1;
        else if(docid== val )
          return ++cursor;
        else
          ++cursor;


      }
    } catch (IOException e) {
      return -1;
    }
    return -1;
  }

  @Override
  public int size() throws IOException
  {

    if(_size==INVALID)
    {
      _size=0;
      DocIdSetIterator it = this.iterator();

      try {
        while(it.nextDoc()!=DocIdSetIterator.NO_MORE_DOCS)
          _size++;
      } catch (IOException e) {
        e.printStackTrace();
        _size = INVALID;
      }

    }
    return _size;
  }
}

