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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.json.JSONObject;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.api.BrowseSelection;
import com.browseengine.bobo.facets.FacetHandler;
import com.senseidb.conf.SenseiFacetHandlerBuilder;
import com.senseidb.search.facet.UIDFacetHandler;
import com.senseidb.util.RequestConverter2;

public class UIDFilterConstructor  extends FilterConstructor{
  public static final String FILTER_TYPE = "ids";

  @Override
  protected Filter doConstructFilter(Object obj) throws Exception {
    final JSONObject json = (JSONObject)obj;
    return new Filter(){

      @Override
      public DocIdSet getDocIdSet(IndexReader reader)
          throws IOException {
        if (reader instanceof BoboIndexReader){
          BoboIndexReader boboReader = (BoboIndexReader)reader;
          FacetHandler uidHandler = boboReader.getFacetHandler(SenseiFacetHandlerBuilder.UID_FACET_NAME);
          if (uidHandler!=null && uidHandler instanceof UIDFacetHandler){
            UIDFacetHandler uidFacet = (UIDFacetHandler)uidHandler;
            try{
              String[] vals = RequestConverter2.getStrings(json.optJSONArray(VALUES_PARAM));
              String[] nots = RequestConverter2.getStrings(json.optJSONArray(EXCLUDES_PARAM));
              BrowseSelection uidSel = new BrowseSelection(SenseiFacetHandlerBuilder.UID_FACET_NAME);
              if (vals != null)
                uidSel.setValues(vals);
              if (nots != null)
                uidSel.setNotValues(nots);
              return uidFacet.buildFilter(uidSel).getDocIdSet(boboReader);
            }
            catch(Exception e){
              throw new IOException(e);
            }
          }
          else{
            throw new IllegalStateException("invalid uid handler "+uidHandler);
          }
        }
        else{
          throw new IllegalStateException("read not instance of "+BoboIndexReader.class);
        }
      }
    
    };
  }
  
}
