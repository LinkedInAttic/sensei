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
package com.senseidb.search.query.filters;

import com.browseengine.bobo.facets.filter.RandomAccessFilter;
import com.browseengine.bobo.util.BigSegmentedArray;
import com.senseidb.util.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.api.BrowseSelection;
import com.browseengine.bobo.api.BrowseSelection.ValueOperation;
import com.browseengine.bobo.facets.FacetHandler;
import com.browseengine.bobo.facets.data.FacetDataCache;
import com.browseengine.bobo.facets.data.TermValueList;
import com.browseengine.bobo.facets.filter.EmptyFilter;
import com.browseengine.bobo.query.MatchAllDocIdSetIterator;
import com.kamikaze.docidset.impl.AndDocIdSet;
import com.kamikaze.docidset.impl.NotDocIdSet;
import com.kamikaze.docidset.impl.OrDocIdSet;

public class SenseiTermFilter extends SenseiFilter {

  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  private static Logger logger = Logger.getLogger(SenseiTermFilter.class);
  
  private final String _name;
  private final String[] _vals;
  private final String[] _not;
  private final boolean _isAnd;
  private final boolean _noAutoOptimize;
  
  public SenseiTermFilter(String name,String vals[],String[] not,boolean isAnd,boolean noAutoOptimize){
    _name = name;
    _vals = vals != null  ? vals : new String[0];
    _not = not != null  ? not : new String[0];

    Arrays.sort(_vals);
    Arrays.sort(_not);
    _isAnd = isAnd;
    _noAutoOptimize = noAutoOptimize;
  }
  
  static DocIdSet buildDefaultDocIdSets(final BoboIndexReader reader,
                                                final String name,
                                                final String[] vals,
                                                boolean isAnd){
    if (vals==null) return null;
    ArrayList<DocIdSet> docSetList = new ArrayList<DocIdSet>(vals.length);

    for (final String val : vals){
      docSetList.add(new DocIdSet() {
        
        @Override
        public DocIdSetIterator iterator() throws IOException {
          return new TermDocIdSetIterator(new Term(name,val), reader);
        }
      });
    }
    
    if (docSetList.size()==1){
      return docSetList.get(0);
    }
    else if (docSetList.size()==0) return null;
    else{
      if (isAnd) {
        return new AndDocIdSet(docSetList);
      }
      else{
        return new OrDocIdSet(docSetList);
      }
    }
  }

  private static int estimateCardinality(int positiveEstimate, int negativeEstimate) {
    if(positiveEstimate > 0) {
      if(negativeEstimate == 0) {
        return positiveEstimate;
      } else {
        // Both positive and negative. We don't know what the cardinality will be before executing the not
        return positiveEstimate;
      }
    } else {
      // Negative only - return the negative estimate
      return negativeEstimate;
    }
  }

  private static class CardinalityComparator implements Comparator<Pair<String, Integer>> {
    @Override
    public int compare(Pair<String, Integer> termA, Pair<String, Integer> termB)
    {
      int comparison = termA.getSecond() - termB.getSecond();
      if(comparison == 0)
      {
        if(termA.getFirst() == null) {
          return termB == null ? 0 : -1;
        }
        return termA.getFirst().compareTo(termB.getFirst());
      }
      else
      {
        return comparison;
      }
    }
  }

  private static CardinalityComparator cardinalityComparator = new CardinalityComparator();


  private static DocIdSet buildLuceneDefaultDocIdSet(final BoboIndexReader reader,
                                                     final String name,
                                                     final String[] vals,
                                                     String[] nots,
                                                     boolean isAnd) throws IOException{
    if (reader.getRuntimeFacetHandlerFactoryMap().containsKey(name))
    {
      // Skip runtime facet handlers
      return new DocIdSet()
      {
        @Override
        public boolean isCacheable()
        {
          return false;
        }

        @Override
        public DocIdSetIterator iterator() throws IOException
        {
          return new MatchAllDocIdSetIterator(reader);
        }
      };
    }
    DocIdSet positiveSet = null;
    DocIdSet negativeSet = null;

    if (vals!=null && vals.length > 0)
      positiveSet = buildDefaultDocIdSets(reader, name, vals, isAnd);

    if (nots!=null && nots.length>0)
      negativeSet = buildDefaultDocIdSets(reader, name, nots, false);

    if (positiveSet!=null){
      if (negativeSet==null){
        return positiveSet;
      }
      else {
        DocIdSet[] sets = new DocIdSet[]{positiveSet,new NotDocIdSet(negativeSet, reader.maxDoc())};
        return new AndDocIdSet(Arrays.asList(sets));
      }
    }
    else{
      if (negativeSet==null){
        return EmptyFilter.getInstance().getRandomAccessDocIdSet(reader);
      }
      else{
        // this could be optimize with AndNot in new Kamikaze
        return new NotDocIdSet(negativeSet, reader.maxDoc());
      }
    }
  }

  static int estimateCardinality(List<Pair<String, Integer>> valsAndFreqs, int maxDoc, boolean isAnd) {
    if(valsAndFreqs == null || valsAndFreqs.isEmpty())
      return 0;

    int cardinality = isAnd ? maxDoc : 0;
    for(Pair<String, Integer> valAndFreq : valsAndFreqs) {
      if(isAnd) {
        cardinality = Math.min(cardinality, valAndFreq.getSecond());
      } else {
        cardinality += valAndFreq.getSecond();
      }
    }

    cardinality = Math.min(cardinality, maxDoc);
    return cardinality;
  }

  @Override
  public SenseiDocIdSet getSenseiDocIdSet(IndexReader reader) throws IOException {
    if (reader instanceof BoboIndexReader){
      BoboIndexReader boboReader = (BoboIndexReader)reader;
      FacetHandler facetHandler = (FacetHandler)boboReader.getFacetHandler(_name);
      Object obj = null;

      List<Pair<String, Integer>> valsAndFreqs = null;
      List<Pair<String, Integer>> notsAndFreqs = null;
      String[] vals = _vals;
      String[] nots = _not;
      int maxDoc = reader.maxDoc();
      int cardinality = maxDoc;
      int notCardinality = 0;

      if (facetHandler != null)
      {
        obj = facetHandler.getFacetData(boboReader);
        if (obj != null && obj instanceof FacetDataCache) {
          FacetDataCache facetData = (FacetDataCache)obj;
          TermValueList valArray = facetData.valArray;
          BigSegmentedArray orderArray = facetData.orderArray;
          int[] freqs = facetData.freqs;

          valsAndFreqs = getValsAndFreqs(vals, valArray, freqs);
          notsAndFreqs = getValsAndFreqs(nots, valArray, freqs);

          vals = getValuesToSearch(valsAndFreqs);
          nots = getValuesToSearch(notsAndFreqs);

          int positiveCardinality = estimateCardinality(valsAndFreqs, maxDoc, _isAnd);
          int negativeCardinality = maxDoc - estimateCardinality(notsAndFreqs,
              maxDoc,
              false);

          cardinality = estimateCardinality(positiveCardinality, negativeCardinality);

          if(_noAutoOptimize) {
            DocIdSet docIdSet = buildLuceneDefaultDocIdSet(boboReader,
                _name,
                vals,
                nots,
                _isAnd);

            return new SenseiDocIdSet(docIdSet, cardinality);
          }
        }
        // we get to optimize using facets
        BrowseSelection sel = new BrowseSelection(_name);


        sel.setValues(vals);
        if (nots != null)
          sel.setNotValues(nots);

        if (_isAnd) {
          sel.setSelectionOperation(ValueOperation.ValueOperationAnd);
        } else {
          sel.setSelectionOperation(ValueOperation.ValueOperationOr);
        }
        RandomAccessFilter filter = facetHandler.buildFilter(sel);
        if (filter == null)
          filter = EmptyFilter.getInstance();

        double facetSelectivity = filter.getFacetSelectivity(boboReader);
        cardinality = (int)(maxDoc * facetSelectivity);

        return new SenseiDocIdSet(filter.getDocIdSet(boboReader), cardinality);
        
      }
      else {
        if (logger.isDebugEnabled()) {
          logger.debug("not facet support, default to term filter: "+_name);
        }

        DocIdSet docIdSet = buildLuceneDefaultDocIdSet(boboReader, _name, vals, nots, _isAnd);

        // Guess cardinality is 50% of documents since we don't have the facet data and because Lucene's TermDocs is
        // too expensive to justify calling
        int cardinalityEstimate = maxDoc >> 1;

        return new SenseiDocIdSet(docIdSet, cardinalityEstimate);
      }
    }
    else{
      throw new IllegalStateException("read not instance of "+BoboIndexReader.class);
    }
  }

  static List<Pair<String, Integer>> getValsAndFreqs(String[] vals, TermValueList valArray, int[] freqs) {
    if(vals == null) {
      return null;
    }

    List<Pair<String, Integer>> valsAndFreqs = new ArrayList<Pair<String, Integer>>(vals.length);


    int offset = 0;
    for (String val : vals) {
      int idx = valArray.indexOf(val);

      if (idx >=0) {
        valsAndFreqs.add(new Pair<String, Integer>(valArray.get(idx), freqs[idx]));
        offset = idx;
      } else {
        offset = -idx - 1;
      }
    }

    // Lowest cardinality docs go first to optimize the AND case
    Collections.sort(valsAndFreqs, cardinalityComparator);
    return valsAndFreqs;
  }

  String[] getValuesToSearch(List<Pair<String, Integer>> valsAndFreqs)
  {
    if(valsAndFreqs == null) {
      return null;
    }

    String[] valuesToSearch = new String[valsAndFreqs.size()];
    int i = 0;
    for(Pair<String, Integer> valAndFreq : valsAndFreqs) {
      valuesToSearch[i++] = valAndFreq.getFirst();
    }
    return valuesToSearch;
  }
}
