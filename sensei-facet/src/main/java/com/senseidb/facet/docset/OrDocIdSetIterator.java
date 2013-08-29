package com.senseidb.facet.docset;

import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;

import java.io.IOException;
import java.util.List;

/**
 * @author Dmytro Ivchenko
 */
public class OrDocIdSetIterator extends DocIdSetIterator {

  private final class Item {
    public final DocIdSetIterator iter;
    public int doc;

    public Item(DocIdSetIterator iter) {
      this.iter = iter;
      this.doc = -1;
    }
  }

  private int _curDoc;
  private final Item[] _heap;
  private int _size;
  private int _cost;

  OrDocIdSetIterator(List<DocIdSet> sets) throws IOException {
    _curDoc = -1;
    _heap = new Item[sets.size()];
    _size = 0;
    for (DocIdSet set : sets) {
      DocIdSetIterator iter = set.iterator();
      _heap[_size++] = new Item(iter != null ? DocIdSet.EMPTY_DOCIDSET.iterator() : iter);
      if (iter != null)
        _cost += set.iterator().cost();
    }
    if (_size == 0) _curDoc = DocIdSetIterator.NO_MORE_DOCS;
  }

  @Override
  public final int docID() {
    return _curDoc;
  }

  @Override
  public final int nextDoc() throws IOException {
    if (_curDoc == DocIdSetIterator.NO_MORE_DOCS) return DocIdSetIterator.NO_MORE_DOCS;

    Item top = _heap[0];
    while (true) {
      DocIdSetIterator topIter = top.iter;
      int docid;
      if ((docid = topIter.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
        top.doc = docid;
        heapAdjust();
      } else {
        heapRemoveRoot();
        if (_size == 0) return (_curDoc = DocIdSetIterator.NO_MORE_DOCS);
      }
      top = _heap[0];
      int topDoc = top.doc;
      if (topDoc > _curDoc) {
        return (_curDoc = topDoc);
      }
    }
  }

  @Override
  public final int advance(int target) throws IOException {
    if (_curDoc == DocIdSetIterator.NO_MORE_DOCS) return DocIdSetIterator.NO_MORE_DOCS;

    if (target <= _curDoc) target = _curDoc + 1;

    Item top = _heap[0];
    while (true) {
      DocIdSetIterator topIter = top.iter;
      int docid;
      if ((docid = topIter.advance(target)) != DocIdSetIterator.NO_MORE_DOCS) {
        top.doc = docid;
        heapAdjust();
      } else {
        heapRemoveRoot();
        if (_size == 0) return (_curDoc = DocIdSetIterator.NO_MORE_DOCS);
      }
      top = _heap[0];
      int topDoc = top.doc;
      if (topDoc >= target) {
        return (_curDoc = topDoc);
      }
    }
  }

  @Override
  public long cost() {
    return _cost;
  }

  // Organize subScorers into a min heap with scorers generating the earlest document on top.
  /*
  private final void heapify() {
      int size = _size;
      for (int i=(size>>1)-1; i>=0; i--)
          heapAdjust(i);
  }
  */
  /* The subtree of subScorers at root is a min heap except possibly for its root element.
   * Bubble the root down as required to make the subtree a heap.
   */
  private final void heapAdjust() {
    final Item[] heap = _heap;
    final Item top = heap[0];
    final int doc = top.doc;
    final int size = _size;
    int i = 0;

    while (true) {
      int lchild = (i << 1) + 1;
      if (lchild >= size) break;

      Item left = heap[lchild];
      int ldoc = left.doc;

      int rchild = lchild + 1;
      if (rchild < size) {
        Item right = heap[rchild];
        int rdoc = right.doc;

        if (rdoc <= ldoc) {
          if (doc <= rdoc) break;

          heap[i] = right;
          i = rchild;
          continue;
        }
      }

      if (doc <= ldoc) break;

      heap[i] = left;
      i = lchild;
    }
    heap[i] = top;
  }

  // Remove the root Scorer from subScorers and re-establish it as a heap
  private final void heapRemoveRoot() {
    _size--;
    if (_size > 0) {
      Item tmp = _heap[0];
      _heap[0] = _heap[_size];
      _heap[_size] = tmp; // keep the finished iterator at the end for debugging
      heapAdjust();
    }
  }

}

