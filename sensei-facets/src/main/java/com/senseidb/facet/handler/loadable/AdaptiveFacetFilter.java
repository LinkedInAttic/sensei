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


import com.senseidb.facet.termlist.TermValueList;
import com.senseidb.facet.docset.EmptyDocIdSet;
import com.senseidb.facet.docset.OrDocIdSet;
import com.senseidb.facet.search.FacetAtomicReader;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class AdaptiveFacetFilter extends Filter
{
  public static final int DEFAULT_INVERTED_INDEX_PENALTY = 32;
	
	private final Filter _facetFilter;
	private final FacetDataCacheBuilder _facetDataCacheBuilder;
	private final Set<String> _valSet;
	private boolean  _takeComplement = false;
  private final int _invertedIndexPenalty;
	
	public interface FacetDataCacheBuilder{
		FacetDataCache build(FacetAtomicReader reader);
		String getName();
		String getIndexFieldName();
	}
	
	// If takeComplement is true, we still return the filter for NotValues . Therefore, the calling function of this class needs to apply NotFilter on top
	// of this filter if takeComplement is true.
	public AdaptiveFacetFilter(FacetDataCacheBuilder facetDataCacheBuilder,
                             Filter facetFilter,
                             List<String> val,
                             boolean takeComplement,
                             int invertedIndexPenalty) {
	  _facetFilter = facetFilter;
	  _facetDataCacheBuilder = facetDataCacheBuilder;
	  _valSet = new HashSet<String>(val);
	  _takeComplement = takeComplement;
    _invertedIndexPenalty = invertedIndexPenalty;
	}
	
	@Override
	public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs)
			throws IOException {
	
      DocIdSet innerDocSet = _facetFilter.getDocIdSet(context, acceptDocs);
      if (innerDocSet == EmptyDocIdSet.getInstance()){
    	  return innerDocSet;
      }
		  
	  FacetDataCache dataCache = _facetDataCacheBuilder.build((FacetAtomicReader)context.reader());
	  int totalCount = context.reader().maxDoc();
	  TermValueList valArray = dataCache.valArray;
	  int freqCount = 0;
	  
	  
	  ArrayList<String> validVals = new ArrayList<String>(_valSet.size());
	  for (String val : _valSet) {
		  int idx = valArray.indexOf(val);
		  if (idx>=0){
			  validVals.add(valArray.get(idx));		// get and format the value
			  freqCount+=dataCache.freqs[idx];
		  }
	  }
	  
	  if (validVals.size()==0){
		  return EmptyDocIdSet.getInstance();
	  }
	  
	  // takeComplement is only used to choose between TermListRandomAccessDocIdSet and innerDocSet
	  int validFreqCount = _takeComplement ? (totalCount - freqCount) : freqCount;

    int invertedIndexCost = estimateInvertedIndexCost(validFreqCount, _valSet.size(), totalCount);
    int forwardIndexCost = estimateForwardIndexCost(validFreqCount, _valSet.size(), totalCount);

	  if (_facetDataCacheBuilder.getIndexFieldName() != null && invertedIndexCost < forwardIndexCost) {
	    return new TermListRandomAccessDocIdSet(_facetDataCacheBuilder.getIndexFieldName(), innerDocSet, validVals, context.reader());
    } else {
		  return innerDocSet;
	  }
	}

  // Merges several streams from lucene
  private final int estimateInvertedIndexCost(int hitCount, int numQueries, int totalDocs) {
    int log2 = BitMath.log2Ceiling(numQueries);
    int numComparisons = Math.max(1, log2);
    return _invertedIndexPenalty * numComparisons * hitCount;
  }

  // Implementation checks in a bitset for each doc
  private final int estimateForwardIndexCost(int hitCount, int numQueries, int totalDocs) {
    return totalDocs;
  }

	public static class TermListRandomAccessDocIdSet extends DocIdSet{

		private final DocIdSet _innerSet;
		private final ArrayList<String> _vals;
		private final AtomicReader _reader;
		private final String _name;

    TermListRandomAccessDocIdSet(String name,DocIdSet innerSet,ArrayList<String> vals, AtomicReader reader){
			_name = name;
			_innerSet = innerSet;
			_vals = vals;
			_reader = reader;
		}
		
		public static class TermDocIdSet extends DocIdSet{
			final Term term;
      private final AtomicReader reader;
			public TermDocIdSet(AtomicReader reader, String name, String val){
				this.reader = reader;
        term = new Term(name,val);
			}
			
			@Override
			public DocIdSetIterator iterator() throws IOException {
				final DocsEnum td = reader.termDocsEnum(term);
				if (td==null)
        {
					return EmptyDocIdSet.getInstance().iterator();
				}
        else
        {
          return td;
        }
			}
		}

		@Override
		public DocIdSetIterator iterator() throws IOException {
			if (_vals.size()==0){
				return EmptyDocIdSet.getInstance().iterator();
			}
			if (_vals.size()==1){
				return new TermDocIdSet(_reader, _name,_vals.get(0)).iterator();
			}
			else{
        ArrayList<DocIdSet> docSetList = new ArrayList<DocIdSet>(_vals.size());
        for (String val : _vals){
          docSetList.add(new TermDocIdSet(_reader, _name,val));
        }
        return new OrDocIdSet(docSetList).iterator();
			}
		}
	}

  private static class BitMath {
    private static final int[] mask = {0x2, 0xC, 0xF0, 0xFF00, 0xFFFF0000};
    private static final int[] shift = {1, 2, 4, 8, 16};

    public static int log2Ceiling(int x) {
      int result = 0;

      boolean isPowerOfTwo = (x & (x - 1)) == 0;

      for (int i = 4; i >= 0; i--) // unroll for speed...
      {
        if ((x & mask[i]) != 0)
        {
          x >>= shift[i];
          result |= shift[i];
        }
      }

      return isPowerOfTwo ? result : result + 1;
    }
  }

}
