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
package com.senseidb.indexing;

import org.apache.lucene.document.Document;
import org.json.JSONException;
import org.json.JSONObject;

import proj.zoie.api.indexing.AbstractZoieIndexable;
import proj.zoie.api.indexing.AbstractZoieIndexableInterpreter;
import proj.zoie.api.indexing.ZoieIndexable;


public abstract class JSONDataInterpreter extends AbstractZoieIndexableInterpreter<JSONObject> {

	@Override
	public ZoieIndexable convertAndInterpret(final JSONObject src) {
		return new AbstractZoieIndexable(){

			@Override
			public IndexingReq[] buildIndexingReqs() {
			  try{
				return new IndexingReq[]{new IndexingReq(buildDoc(src))};
			  }
			  catch(JSONException e){
				throw new RuntimeException(e.getMessage(),e);
			  }
			}

      //@Override
      //public byte[] getStoreValue()
      //{
        //throw new NotImplementedException();
      //}

      @Override
			public long getUID() {
			  try{
				return extractUID(src);
			  }
			  catch(JSONException e){
				throw new RuntimeException(e.getMessage(),e);
			  }
			}

			@Override
			public boolean isDeleted() {
				return extractDeleteFlag(src);
			}

			@Override
			public boolean isSkip() {
				return extractSkipFlag(src);
			}
			
		};
	}

	public abstract long extractUID(JSONObject obj) throws JSONException;
	public abstract Document buildDoc(JSONObject obj) throws JSONException;
	
	public boolean extractSkipFlag(JSONObject obj){
		return false;
	}
	
	public boolean extractDeleteFlag(JSONObject obj){
		return false;
	}
}
