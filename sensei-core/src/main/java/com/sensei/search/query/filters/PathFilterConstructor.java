package com.sensei.search.query.filters;

import java.io.IOException;
import java.util.Iterator;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.json.JSONObject;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.api.BrowseSelection;
import com.browseengine.bobo.facets.impl.PathFacetHandler;
import com.browseengine.bobo.facets.FacetHandler;

import com.browseengine.bobo.query.MatchAllDocIdSetIterator;

public class PathFilterConstructor extends FilterConstructor
{
  public static final String FILTER_TYPE = "path";

  @Override
  protected Filter doConstructFilter(Object obj) throws Exception
  {
    JSONObject json = (JSONObject)obj;

    Iterator<String> iter = json.keys();
    if (!iter.hasNext())
      return null;

    final String field = iter.next();
    final String path = json.getString(field);

    return new Filter()
    {
      @Override
      public DocIdSet getDocIdSet(final IndexReader reader) throws IOException
      {
        if (reader instanceof BoboIndexReader)
        {
          BoboIndexReader boboReader = (BoboIndexReader)reader;
          FacetHandler facetHandler = boboReader.getFacetHandler(field);
          if (facetHandler != null && facetHandler instanceof PathFacetHandler)
          {
            BrowseSelection sel = new BrowseSelection(field);
            sel.setValues(new String[]{path});
            Filter filter = ((PathFacetHandler)facetHandler).buildFilter(sel);
            if (filter == null)
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
            return filter.getDocIdSet(reader);
          }
        }

        throw new UnsupportedOperationException("Path filter is not supported for your field: " + field);
      }
    };
  }
}
