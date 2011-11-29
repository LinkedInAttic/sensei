package com.sensei.search.query.filters;

import java.io.IOException;
import java.util.Iterator;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.TermRangeFilter;
import org.json.JSONObject;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.facets.FacetHandler;

import com.browseengine.bobo.facets.filter.FacetRangeFilter;
import com.browseengine.bobo.query.MatchAllDocIdSetIterator;

public class RangeFilterConstructor extends FilterConstructor
{
  public static final String FILTER_TYPE = "range";

  @Override
  protected Filter doConstructFilter(Object obj) throws Exception
  {
    JSONObject json = (JSONObject)obj;

    Iterator<String> iter = json.keys();
    if (!iter.hasNext())
      return null;

    final String field = iter.next();

    JSONObject jsonObj = json.getJSONObject(field);

    final String from          = jsonObj.optString(FROM_PARAM);
    final String to            = jsonObj.optString(TO_PARAM);
    final boolean noOptimize   = jsonObj.optBoolean(NOOPTIMIZE_PARAM, false);

    return new Filter()
    {
      @Override
      public DocIdSet getDocIdSet(final IndexReader reader) throws IOException
      {
        if (!noOptimize)
        {
          if (reader instanceof BoboIndexReader)
          {
            BoboIndexReader boboReader = (BoboIndexReader)reader;
            FacetHandler facetHandler = boboReader.getFacetHandler(field);
            if (facetHandler != null)
            {
              StringBuilder sb = new StringBuilder("[");
              if (from == null)
                sb.append("*");
              else
                sb.append(from);
              sb.append(" TO ");
              if (to == null)
                sb.append("*");
              else
                sb.append(to);
              sb.append("]");

              FacetRangeFilter filter = new FacetRangeFilter(facetHandler, sb.toString());
              return filter.getDocIdSet(reader);
            }
          }
        }
        if (from == null)
          if (to == null)
            return new DocIdSet()
            {
              @Override
              public boolean isCacheable()
              {
                return true;
              }

              @Override
              public DocIdSetIterator iterator() throws IOException
              {
                return new MatchAllDocIdSetIterator(reader);
              }
            };
          else
            return new TermRangeFilter(field, from, to, false, true).getDocIdSet(reader);
        else if (to == null)
          return new TermRangeFilter(field, from, to, true, false).getDocIdSet(reader);

        return new TermRangeFilter(field, from, to, true, true).getDocIdSet(reader);
      }
    };
  }
}
