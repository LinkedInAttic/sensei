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

import java.io.IOException;
import java.util.Iterator;

import com.browseengine.bobo.facets.filter.RandomAccessFilter;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
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
  protected SenseiFilter doConstructFilter(Object param) throws Exception
  {
    JSONObject json = (JSONObject)param;

    Iterator<String> iter = json.keys();
    if (!iter.hasNext())
      return null;

    final String field = iter.next();
    final String path;
    final int depth;
    final boolean strict;

    Object obj = json.get(field);
    if (obj instanceof JSONObject)
    {
      path   = ((JSONObject)obj).getString(VALUE_PARAM);
      depth  = ((JSONObject)obj).optInt(DEPTH_PARAM, 0);
      strict = ((JSONObject)obj).optBoolean(STRICT_PARAM, false);
    }
    else
    {
      path   = String.valueOf(obj);
      depth  = 0;
      strict = false;
    }

    return new SenseiFilter()
    {
      @Override
      public SenseiDocIdSet getSenseiDocIdSet(final IndexReader reader) throws IOException {
        if (reader instanceof BoboIndexReader)
        {
          BoboIndexReader boboReader = (BoboIndexReader)reader;
          FacetHandler facetHandler = boboReader.getFacetHandler(field);
          if (facetHandler != null && facetHandler instanceof PathFacetHandler)
          {
            BrowseSelection sel = new BrowseSelection(field);
            sel.setValues(new String[]{path});
            sel.setSelectionProperty(PathFacetHandler.SEL_PROP_NAME_DEPTH, String.valueOf(depth));
            sel.setSelectionProperty(PathFacetHandler.SEL_PROP_NAME_STRICT, String.valueOf(strict));
            RandomAccessFilter filter = ((PathFacetHandler)facetHandler).buildFilter(sel);
            if (filter == null) {

              DocIdSet docIdSet = new DocIdSet() {
                @Override
                public boolean isCacheable() {
                  return false;
                }

                @Override
                public DocIdSetIterator iterator() throws IOException {
                  return new MatchAllDocIdSetIterator(reader);
                }
              };
              int maxDoc = reader.maxDoc();
              return new SenseiDocIdSet(docIdSet, DocIdSetCardinality.one(), "ALL");
            }
            return SenseiDocIdSet.build(filter, boboReader, "PATH " + field);
          }
        }

        throw new UnsupportedOperationException("Path filter is not supported for your field: " + field);
      }
    };
  }
}
