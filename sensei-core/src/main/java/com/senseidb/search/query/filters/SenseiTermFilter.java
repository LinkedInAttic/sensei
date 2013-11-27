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
import com.senseidb.search.facet.UIDFacetHandler;
import com.senseidb.util.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.lang.StringUtils;
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

    // Bobo silliness: Empty vals means match all, which technically means an AND of an empty set.
    // EXCEPT if nots are also empty, but this is handled bellow.
    _isAnd = isAnd || vals == null || vals.length == 0;
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

  public String planString(String type, String[] vals, String[] nots, List<String> optimizedVals, List<String> optimizedNots) {
    StringBuilder plan = new StringBuilder();
    boolean first = false;

    plan.append(_name);
    plan.append(" ");
    plan.append(type);
    plan.append(_isAnd ? " CONTAINS ALL <" : " IN <");
    plan.append(StringUtils.join(vals, ", "));
    if (!optimizedVals.isEmpty()) {
      first = vals.length == 0;
      for (String optimized: optimizedVals) {
        if (first) {
          first = false;
        } else {
          plan.append(", ");
        }
        plan.append(optimized);
        plan.append('*');
      }
    }
    plan.append("> EXCEPT <");
    plan.append(StringUtils.join(nots, ", "));
    if (!optimizedNots.isEmpty()) {
      first = vals.length == 0;
      for (String optimized: optimizedNots) {
        if (first) {
          first = false;
        } else {
          plan.append(", ");
        }
        plan.append(optimized);
        plan.append('*');
      }
    }
    plan.append(">");
    return plan.toString();
  }


  @Override
  public SenseiDocIdSet getSenseiDocIdSet(IndexReader reader) throws IOException {
    if (reader instanceof BoboIndexReader) {
      BoboIndexReader boboReader = (BoboIndexReader)reader;
      FacetHandler facetHandler = (FacetHandler)boboReader.getFacetHandler(_name);
      Object obj = null;

      String[] vals = _vals;
      String[] nots = _not;
      List<String> optimizedVals = new ArrayList<String>(vals.length);
      List<String> optimizedNots = new ArrayList<String>(nots.length);
      int maxDoc = reader.maxDoc();

      if ( (vals == null || vals.length == 0) && (nots == null || nots.length == 0) ) {
        // Bobo madness part 2: no vals and no nots will match nothing, regardless of isAnd.
        return SenseiDocIdSet.buildMatchNone(planString("TRIVIAL", vals, nots, optimizedVals, optimizedNots));
      }

      // No facetHandler == no cardinality info.
      DocIdSetCardinality totalDocIdSetCardinality = null;
      String planType = "FACETED NOFACETDATA";

      if(facetHandler == null) {
        if (logger.isDebugEnabled()) {
          logger.debug("not facet support, default to term filter: "+_name);
        }

        DocIdSet docIdSet = buildLuceneDefaultDocIdSet(boboReader, _name, vals, nots, _isAnd);

        // No cardinality since we don't have the facet data and because Lucene's TermDocs is
        // too expensive to justify calling
        return new SenseiDocIdSet(docIdSet, DocIdSetCardinality.random(), planString("NOFACET LUCENE", vals, nots, optimizedVals, optimizedNots));
      } else if (facetHandler instanceof UIDFacetHandler) {
        planType = "FACET UID";

        if (vals.length != 0) {
          // We *could* look up all the ranges right now and see if there's any one even there. This would greatly
          // speed up empty _uid queries, but I've never seen one of those.
          totalDocIdSetCardinality = DocIdSetCardinality.exactRange(0, 1, maxDoc);
        } else {
          totalDocIdSetCardinality = DocIdSetCardinality.zero();
        }
        if (nots.length != 0) {
          totalDocIdSetCardinality.andWith(DocIdSetCardinality.exactRange(maxDoc - nots.length, maxDoc, maxDoc));
        }
      } else {
        obj = facetHandler.getFacetData(boboReader);
        if (obj != null && obj instanceof FacetDataCache) {
          planType = "FACETED";

          FacetDataCache facetData = (FacetDataCache)obj;
          TermValueList valArray = facetData.valArray;
          int[] freqs = facetData.freqs;

          // Total cardinality = AND/OR(val1, val2, ...) AND NOT (OR(not1, not2))
          totalDocIdSetCardinality = _isAnd ? DocIdSetCardinality.one() : DocIdSetCardinality.zero();
          vals = getValsByFrequency(vals, freqs, maxDoc, totalDocIdSetCardinality, valArray, optimizedVals, _isAnd);

          DocIdSetCardinality notDocIdSetCardinality = DocIdSetCardinality.zero();
          nots = getValsByFrequency(nots, freqs, maxDoc, notDocIdSetCardinality, valArray, optimizedNots, false);
          notDocIdSetCardinality.invert();
          totalDocIdSetCardinality.andWith(notDocIdSetCardinality);

          // If we optimized it out completely, return trivial sets. This is mostly there to deal with weird
          // semantics for empty-match filters in Bobo.
          if (totalDocIdSetCardinality.isOne()) {
            return SenseiDocIdSet.buildMatchAll(reader, planString("FACET TRIVIAL", vals, nots, optimizedVals, optimizedNots));
          } else if (totalDocIdSetCardinality.isZero()) {
            return SenseiDocIdSet.buildMatchNone(planString("FACET TRIVIAL", vals, nots, optimizedVals, optimizedNots));
          }

          if(_noAutoOptimize) {
            DocIdSet docIdSet = buildLuceneDefaultDocIdSet(boboReader,
                _name,
                vals,
                nots,
                _isAnd);

            return new SenseiDocIdSet(docIdSet, totalDocIdSetCardinality, planString("DE-OPTIMIZED LUCENE", vals, nots, optimizedVals, optimizedNots));
          }
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

      // If we don't have an cardinality estimate, ask Bobo.
      if (totalDocIdSetCardinality == null) {
        totalDocIdSetCardinality = DocIdSetCardinality.exact(filter.getFacetSelectivity(boboReader));
        // Zero means 'delete', and I don't trust Bobo enough.
        if (totalDocIdSetCardinality.isZero()) {
          totalDocIdSetCardinality = DocIdSetCardinality.exactRange(0.0, 0.001);
        }
      }

      return new SenseiDocIdSet(filter.getDocIdSet(boboReader), totalDocIdSetCardinality, planString(planType, vals, nots, optimizedVals, optimizedNots));

    } else{
      throw new IllegalStateException("read not instance of "+BoboIndexReader.class);
    }
  }

  private static final Comparator<Pair<String, DocIdSetCardinality>> DECREASING_CARDINALITY_COMPARATOR = new Comparator<Pair<String, DocIdSetCardinality>>() {
    @Override
    public int compare(Pair<String, DocIdSetCardinality> a, Pair<String, DocIdSetCardinality> b) {
      return -a.getSecond().compareTo(b.getSecond());
    }
  };
  public static final Comparator<Pair<String, DocIdSetCardinality>> INCREASING_CARDINALITY_COMPARATOR = new Comparator<Pair<String, DocIdSetCardinality>> (){
    @Override
    public int compare(Pair<String, DocIdSetCardinality> a, Pair<String, DocIdSetCardinality> b) {
      return a.getSecond().compareTo(b.getSecond());
    }
  };

  /* Get the list of values, sorted by frequency.
  *
  * ANDs will be sorted by increasing frequency, ORs by decreasing.
  * We skip terms in the AND which match all docs. We skip terms in OR which match all docs.
  * We update total cardinality as we go, but it's supposed to be initialized to 1 for ANDs, 0 for ORs.
  */
  static String[] getValsByFrequency(String[] vals, int[] freqs, int maxDoc, DocIdSetCardinality total, TermValueList valArray, List<String> optimizedOut, boolean isAnd) {
    List<Pair<String, DocIdSetCardinality>> valsAndFreqs = new ArrayList<Pair<String, DocIdSetCardinality>>(vals.length);

    for (String val : vals) {
      int i = valArray.indexOf(val);

      if (i >=0) {
        DocIdSetCardinality docIdSetCardinality = DocIdSetCardinality.exact(((double) freqs[i]) / maxDoc);
        if (isAnd) {
          if (docIdSetCardinality.isOne()) {
            optimizedOut.add(val);
            continue;
          }
          total.andWith(docIdSetCardinality);
        } else {
          if (docIdSetCardinality.isZero()) {
            optimizedOut.add(val);
            continue;
          }
          total.orWith(docIdSetCardinality);
        }
        valsAndFreqs.add(new Pair<String, DocIdSetCardinality>(valArray.get(i), docIdSetCardinality));
      }
    }

    // Lowest cardinality docs go first to optimize the AND case, last for the OR case.
    Collections.sort(valsAndFreqs, isAnd ? INCREASING_CARDINALITY_COMPARATOR : DECREASING_CARDINALITY_COMPARATOR);

    String[] sortedVals = new String[valsAndFreqs.size()];
    int i = 0;
    while (i < sortedVals.length) {
      sortedVals[i] = valsAndFreqs.get(i).getFirst();
      ++i;
    }
    return sortedVals;
  }
}
