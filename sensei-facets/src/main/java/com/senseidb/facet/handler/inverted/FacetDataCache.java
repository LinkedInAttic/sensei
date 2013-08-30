/**
 * This software is licensed to you under the Apache License, Version 2.0 (the
 * "Apache License").
 *
 * LinkedIn's contributions are made under the Apache License. If you contribute
 * to the Software, the contributions will be deemed to have been made under the
 * Apache License, unless you expressly indicate otherwise. Please do not make any
 * contributions that would be inconsistent with the Apache License.
 *
 * You may obtain a copy of the Apache License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, this software
 * distributed under the Apache License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the Apache
 * License for the specific language governing permissions and limitations for the
 * software governed under the Apache License.
 *
 * © 2012 LinkedIn Corp. All Rights Reserved.  
 */

package com.senseidb.facet.handler.inverted;


import com.senseidb.facet.data.BigByteArray;
import com.senseidb.facet.data.BigIntArray;
import com.senseidb.facet.data.BigSegmentedArray;
import com.senseidb.facet.data.BigShortArray;
import com.senseidb.facet.termlist.TermListFactory;
import com.senseidb.facet.termlist.TermStringList;
import com.senseidb.facet.termlist.TermValueList;
import com.senseidb.facet.handler.FacetHandler;
import com.senseidb.facet.search.FacetAtomicReader;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldComparatorSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FacetDataCache<T> {

  public BigSegmentedArray orderArray;
  public TermValueList<T> valArray;
  public int[] freqs;
  public int[] minIDs;
  public int[] maxIDs;

  public FacetDataCache(BigSegmentedArray orderArray, TermValueList<T> valArray, int[] freqs, int[] minIDs,
                        int[] maxIDs) {
    this.orderArray = orderArray;
    this.valArray = valArray;
    this.freqs = freqs;
    this.minIDs = minIDs;
    this.maxIDs = maxIDs;
  }

  public FacetDataCache() {
    this.orderArray = null;
    this.valArray = null;
    this.maxIDs = null;
    this.minIDs = null;
    this.freqs = null;
  }

  public int getNumItems(int docid) {
    int valIdx = orderArray.get(docid);
    return valIdx <= 0 ? 0 : 1;
  }

  private final static BigSegmentedArray newInstance(int termCount, int maxDoc) {
    // we use < instead of <= to take into consideration "missing" value (zero element in the dictionary)
    if (termCount < Byte.MAX_VALUE) {
      return new BigByteArray(maxDoc);
    } else if (termCount < Short.MAX_VALUE) {
      return new BigShortArray(maxDoc);
    } else
      return new BigIntArray(maxDoc);
  }

  protected int getDictValueCount(AtomicReader reader, String field) throws IOException {
    int ret = 0;   
    Terms terms = reader.terms(field);
    if (terms != null) {
      TermsEnum termEnum = terms.iterator(null);
      while (termEnum.next() != null) {
        ret++;
      }
    }
    return ret;
  }
  protected int getNegativeValueCount(AtomicReader reader, String field) throws IOException {
    int ret = 0;
    Terms terms = reader.terms(field);
    if (terms != null) {
      TermsEnum termEnum = terms.iterator(null);
      while (termEnum.next() != null) {
        String term = termEnum.term().utf8ToString();
        if (term == null || !term.equals(field))
          break;
        if (!term.startsWith("-")) {
          break;
        }
        ret++;
      }
    }
    return ret;
  }
  public void load(String fieldName, AtomicReader reader, TermListFactory<T> listFactory) throws IOException {
    String field = fieldName.intern();
    int maxDoc = reader.maxDoc();

    BigSegmentedArray order = this.orderArray;
    if (order == null) // we want to reuse the memory
    {
        int dictValueCount = getDictValueCount(reader, fieldName);
        order = newInstance(dictValueCount, maxDoc);
    } else {
      order.ensureCapacity(maxDoc); // no need to fill to 0, we are reseting the
                                    // data anyway
    }
    this.orderArray = order;
    
    IntArrayList minIDList = new IntArrayList();
    IntArrayList maxIDList = new IntArrayList();
    IntArrayList freqList = new IntArrayList();

    int length = maxDoc + 1;
    TermValueList<T> list = listFactory == null ? (TermValueList<T>) new TermStringList() : listFactory
        .createTermList();
    int negativeValueCount = getNegativeValueCount(reader, field); 

    Terms terms = reader.terms(field);
    int t = 0; // current term number

    list.add(null);
    minIDList.add(-1);
    maxIDList.add(-1);
    freqList.add(0);
    int totalFreq = 0;    
    // int df = 0;
    t++;
    if (terms != null) {
      TermsEnum termsEnum = terms.iterator(null);
      while (termsEnum.next() != null) {
        String term = termsEnum.term().utf8ToString();

        // store term text
        // we expect that there is at most one term per document
        if (t >= length)
          throw new RuntimeException("there are more terms than " + "documents in field \"" + field
              + "\", but it's impossible to sort on " + "tokenized fields");
        list.add(term);

        // freqList.add(termEnum.docFreq()); // doesn't take into account
        // deldocs
        int minID = -1;
        int maxID = -1;
        int df = 0;
        int valId = (t - 1 < negativeValueCount) ? (negativeValueCount - t + 1) : t;

        DocsEnum docsEnum = reader.termDocsEnum(new Term(field, termsEnum.term()));
        if (docsEnum != null) {
          if (docsEnum != null && docsEnum.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
            df++;
            int docid = docsEnum.docID();
            order.add(docid, valId);
            minID = docid;
            while (docsEnum.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
              df++;
              docid = docsEnum.docID();
              order.add(docid, valId);
            }
            maxID = docid;
          }
        }
        freqList.add(df);
        totalFreq += df;
        minIDList.add(minID);
        maxIDList.add(maxID);

        t++;
      }
    }
    list.seal();
    this.valArray = list;
    this.freqs = freqList.toIntArray();
    this.minIDs = minIDList.toIntArray();
    this.maxIDs = maxIDList.toIntArray();

    int doc = 0;
    while (doc <= maxDoc && order.get(doc) != 0) {
      ++doc;
    }
    if (doc <= maxDoc) {
      this.minIDs[0] = doc;
      // Try to get the max
      doc = maxDoc;
      while (doc > 0 && order.get(doc) != 0) {
        --doc;
      }
      if (doc > 0) {
        this.maxIDs[0] = doc;
      }
    }
    this.freqs[0] = maxDoc + 1 - totalFreq;
  }

  private static int[] convertString(FacetDataCache dataCache, List<String> vals) {
    IntList list = new IntArrayList(vals.size());
    for (int i = 0; i < vals.size(); ++i) {
      int index = dataCache.valArray.indexOf(vals.get(i));
      if (index >= 0) {
        list.add(index);
      }
    }
    return list.toIntArray();
  }

  /**
   * Same as convert(FacetDataCache dataCache,String[] vals) except that the
   * values are supplied in raw form so that we can take advantage of the type
   * information to find index faster.
   * 
   * @param <T>
   * @param dataCache
   * @param vals
   * @return the array of order indices of the values.
   */
  public static <T> int[] convert(FacetDataCache<T> dataCache, List<T> vals, Class<T> clazz) {
    if (vals != null && (clazz.equals(String.class)))
      return convertString(dataCache, (List<String>)vals);
    IntList list = new IntArrayList(vals.size());
    for (int i = 0; i < vals.size(); ++i) {
      int index = dataCache.valArray.indexOfWithType(vals.get(i));
      if (index >= 0) {
        list.add(index);
      }
    }
    return list.toIntArray();
  }

  public static class FacetFieldComparatorSource extends FieldComparatorSource {
    private FacetHandler<FacetDataCache> _facetHandler;

    public FacetFieldComparatorSource(FacetHandler<FacetDataCache> facetHandler) {
      _facetHandler = facetHandler;
    }

    @Override
    public FieldComparator newComparator(String fieldname, int numHits, int sortPos, boolean reversed) throws IOException {
      return new FacetFieldComparator(_facetHandler, numHits, reversed);
    }
  }

  private static class FacetFieldComparator<T> extends FieldComparator<T>
  {
    private Comparable[] _hits;
    private BigSegmentedArray _orderArray;
    private TermValueList<T> _valArray;
    private FacetHandler<FacetDataCache<T>> _handler;
    private Comparable _bottom;
    private int _reversed; // use int for better pipelining

    public FacetFieldComparator(FacetHandler<FacetDataCache<T>> handler, int numHits, boolean reversed) {
      _hits = new Comparable[numHits];
      _handler = handler;
      _reversed = reversed ? -1 : 1;
    }

    @Override
    public int compare(int slot1, int slot2) {
      int ret = _hits[slot1].compareTo(_hits[slot2]);
      return ret * _reversed;
    }

    @Override
    public void setBottom(int slot) {
      _bottom = _hits[slot];
    }

    @Override
    public int compareBottom(int doc) throws IOException {
      int ret = _bottom.compareTo(_valArray.getComparableValue(_orderArray.get(doc)));
      return ret * _reversed;
    }

    @Override
    public void copy(int slot, int doc) throws IOException {
      _hits[slot] = _valArray.getComparableValue(_orderArray.get(doc));
    }

    @Override
    public FieldComparator<T> setNextReader(AtomicReaderContext context) throws IOException {
      FacetAtomicReader reader = (FacetAtomicReader)context.reader();
      FacetDataCache<T> dataCache = _handler.getFacetData(reader);
      _orderArray = dataCache.orderArray;
      _valArray = dataCache.valArray;
      return this;
    }

    @Override
    public T value(int slot) {
      return (T)_hits[slot];
    }

    @Override
    public int compareDocToValue(int doc, Object value) throws IOException {
      int ret = _valArray.getComparableValue(_orderArray.get(doc)).compareTo(value);
      return ret * _reversed;
    }
  };

}
