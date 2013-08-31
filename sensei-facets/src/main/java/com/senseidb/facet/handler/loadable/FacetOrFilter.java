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

package com.senseidb.facet.handler.loadable;

import com.senseidb.facet.data.BigSegmentedArray;
import com.senseidb.facet.docset.EmptyDocIdSet;
import com.senseidb.facet.handler.FacetHandler;
import com.senseidb.facet.handler.LoadableFacetHandler;
import com.senseidb.facet.search.FacetAtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.OpenBitSet;

import java.io.IOException;
import java.util.List;

public class FacetOrFilter extends Filter {
  protected final LoadableFacetHandler<FacetDataCache> _facetHandler;
  protected final List<String> _vals;
  private final boolean _takeCompliment;

  public FacetOrFilter(LoadableFacetHandler<FacetDataCache> facetHandler, List<String> vals) {
    this(facetHandler, vals, false);
  }

  public FacetOrFilter(LoadableFacetHandler<FacetDataCache> facetHandler, List<String> vals, boolean takeCompliment) {
    _facetHandler = facetHandler;
    _vals = vals;
    _takeCompliment = takeCompliment;
  }

  public double getFacetSelectivity(FacetAtomicReader reader) {
    double selectivity = 0;
    FacetDataCache dataCache = _facetHandler.getFacetData(reader);
    int accumFreq = 0;
    for (String value : _vals) {
      int idx = dataCache.valArray.indexOf(value);
      if (idx < 0) {
        continue;
      }
      accumFreq += dataCache.freqs[idx];
    }
    int total = reader.maxDoc();
    selectivity = (double) accumFreq / (double) total;
    if (selectivity > 0.999) {
      selectivity = 1.0;
    }
    if (_takeCompliment) {
      selectivity = 1.0 - selectivity;
    }
    return selectivity;
  }

  @Override
  public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {

    if (_vals.size() == 0) {
      return EmptyDocIdSet.getInstance();
    } else {
      return new FacetOrDocIdSet(_facetHandler, (FacetAtomicReader)context.reader(), _vals, _takeCompliment);
    }
  }

  public static class FacetOrDocIdSet extends DocIdSet {

    private OpenBitSet _bitset;
    private final BigSegmentedArray _orderArray;
    private final FacetDataCache _dataCache;
    private final int[] _index;

    FacetOrDocIdSet(LoadableFacetHandler<FacetDataCache> facetHandler, FacetAtomicReader reader,
                                List<String> vals, boolean takeCompliment) {
      _dataCache = facetHandler.getFacetData(reader);
      _orderArray = _dataCache.orderArray;
      _index = FacetDataCache.convert(_dataCache, vals, String.class);

      _bitset = new OpenBitSet(_dataCache.valArray.size());
      for (int i : _index) {
        _bitset.fastSet(i);
      }

      if (takeCompliment) {
        // flip the bits
        for (int i = 0; i < _dataCache.valArray.size(); ++i) {
          _bitset.fastFlip(i);
        }
      }
    }

    @Override
    public DocIdSetIterator iterator() throws IOException {
      return new FacetOrDocIdSetIterator(_dataCache, _bitset);
    }

  }

  public static class FacetOrDocIdSetIterator extends DocIdSetIterator {
    protected int _doc;
    protected final FacetDataCache _dataCache;
    protected int _maxID;
    protected final OpenBitSet _bitset;
    protected final BigSegmentedArray _orderArray;

    public FacetOrDocIdSetIterator(FacetDataCache dataCache, OpenBitSet bitset) {
      _dataCache = dataCache;
      _orderArray = dataCache.orderArray;
      _bitset = bitset;

      _doc = Integer.MAX_VALUE;
      _maxID = -1;
      int size = _dataCache.valArray.size();
      for (int i = 0; i < size; ++i) {
        if (!bitset.fastGet(i)) {
          continue;
        }
        if (_doc > _dataCache.minIDs[i]) {
          _doc = _dataCache.minIDs[i];
        }
        if (_maxID < _dataCache.maxIDs[i]) {
          _maxID = _dataCache.maxIDs[i];
        }
      }
      _doc--;
      if (_doc < 0) _doc = -1;
    }

    @Override
    final public int docID() {
      return _doc;
    }

    @Override
    public int nextDoc() throws IOException {
      return (_doc = (_doc < _maxID ? _orderArray.findValues(_bitset, (_doc + 1), _maxID) : NO_MORE_DOCS));
    }

    @Override
    public int advance(int id) throws IOException {
      if (_doc < id) {
        return (_doc = (id <= _maxID ? _orderArray.findValues(_bitset, id, _maxID) : NO_MORE_DOCS));
      }
      return nextDoc();
    }

    @Override
    public long cost() {
      return _bitset.size();
    }
  }

}
