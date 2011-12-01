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

    final String  from, to;
    final boolean include_lower, include_upper;
    final boolean noOptimize = jsonObj.optBoolean(NOOPTIMIZE_PARAM, false);

    String gt  = jsonObj.optString(GT_PARAM, null);
    String gte = jsonObj.optString(GTE_PARAM, null);
    String lt  = jsonObj.optString(LT_PARAM, null);
    String lte = jsonObj.optString(LTE_PARAM, null);

    if (gt != null && gt.length() != 0)
    {
      from          = gt;
      include_lower = false;
    }
    else if (gte != null && gte.length() != 0)
    {
      from          = gte;
      include_lower = true;
    }
    else
    {
      from          = jsonObj.optString(FROM_PARAM, null);
      include_lower = jsonObj.optBoolean(INCLUDE_LOWER_PARAM, true);
    }

    if (lt != null && lt.length() != 0)
    {
      to = lt;
      include_upper = false;
    }
    else if (lte != null && lte.length() != 0)
    {
      to = lte;
      include_upper = true;
    }
    else
    {
      to            = jsonObj.optString(TO_PARAM, null);
      include_upper = jsonObj.optBoolean(INCLUDE_UPPER_PARAM, true);
    }

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
              StringBuilder sb = new StringBuilder();
              if (include_lower && from != null && from.length() != 0)
                sb.append("[");
              else
                sb.append("(");

              if (from == null || from.length() == 0)
                sb.append("*");
              else
                sb.append(from);
              sb.append(" TO ");
              if (to == null || to.length() == 0)
                sb.append("*");
              else
                sb.append(to);

              if (include_upper && to != null && to.length() != 0)
                sb.append("]");
              else
                sb.append(")");

              FacetRangeFilter filter = new FacetRangeFilter(facetHandler, sb.toString());
              return filter.getDocIdSet(reader);
            }
          }
        }
        if (from == null || from.length() == 0)
          if (to == null || to.length() == 0)
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
          else
            return new TermRangeFilter(field, from, to, false, include_upper).getDocIdSet(reader);
        else if (to == null|| to.length() == 0)
          return new TermRangeFilter(field, from, to, include_lower, false).getDocIdSet(reader);

        return new TermRangeFilter(field, from, to, include_lower, include_upper).getDocIdSet(reader);
      }
    };
  }
}
