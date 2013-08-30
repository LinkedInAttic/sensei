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

import com.senseidb.facet.data.BigSegmentedArray;
import com.senseidb.facet.docset.EmptyDocIdSet;
import com.senseidb.facet.handler.FacetHandler;
import com.senseidb.facet.search.FacetAtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;

import java.io.IOException;

public class FacetFilter extends Filter {
  private final FacetHandler<FacetDataCache> _facetHandler;
  protected final String _value;

  public FacetFilter(FacetHandler<FacetDataCache> facetHandler, String value) {
    _facetHandler = facetHandler;
    _value = value;
  }

  public double getFacetSelectivity(FacetAtomicReader reader) {
    double selectivity = 0;
    FacetDataCache dataCache = _facetHandler.getFacetData(reader);
    int idx = dataCache.valArray.indexOf(_value);
    if (idx < 0) {
      return 0.0;
    }
    int freq = dataCache.freqs[idx];
    int total = reader.maxDoc();
    selectivity = (double) freq / (double) total;
    return selectivity;
  }

  public static class FacetDocIdSetIterator extends DocIdSetIterator {
    protected int _doc;
    protected final int _index;
    protected final int _maxID;
    protected final BigSegmentedArray _orderArray;

    public FacetDocIdSetIterator(FacetDataCache dataCache, int index) {
      _index = index;
      _doc = Math.max(-1, dataCache.minIDs[_index] - 1);
      _maxID = dataCache.maxIDs[_index];
      _orderArray = dataCache.orderArray;
    }

    @Override
    final public int docID() {
      return _doc;
    }

    @Override
    public int nextDoc() throws IOException {
      return (_doc = (_doc < _maxID ? _orderArray.findValue(_index, (_doc + 1), _maxID) : NO_MORE_DOCS));
    }

    @Override
    public int advance(int id) throws IOException {
      if (_doc < id) {
        return (_doc = (id <= _maxID ? _orderArray.findValue(_index, id, _maxID) : NO_MORE_DOCS));
      }
      return nextDoc();
    }

    @Override
    public long cost() {
      return 1;
    }
  }

  @Override
  public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
    FacetDataCache dataCache = _facetHandler.getFacetData((FacetAtomicReader) context.reader());
    int index = dataCache.valArray.indexOf(_value);
    if (index < 0) {
      return EmptyDocIdSet.getInstance();
    } else {
      return new FacetDataRandomAccessDocIdSet(dataCache, index);
    }
  }

  public static class FacetDataRandomAccessDocIdSet extends DocIdSet {

    private final FacetDataCache _dataCache;
    private final BigSegmentedArray _orderArray;
    private final int _index;

    FacetDataRandomAccessDocIdSet(FacetDataCache dataCache, int index) {
      _dataCache = dataCache;
      _orderArray = _dataCache.orderArray;
      _index = index;
    }

    @Override
    public DocIdSetIterator iterator() throws IOException {
      return new FacetDocIdSetIterator(_dataCache, _index);
    }

  }
}
