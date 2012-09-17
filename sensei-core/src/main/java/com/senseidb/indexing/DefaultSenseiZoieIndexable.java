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

import java.lang.reflect.Method;
import java.text.Format;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.Field.TermVector;

import proj.zoie.api.indexing.AbstractZoieIndexable;
import proj.zoie.api.indexing.ZoieIndexable.IndexingReq;

import com.senseidb.indexing.DefaultSenseiInterpreter.IndexSpec;
import com.senseidb.indexing.DefaultSenseiInterpreter.MetaFormatSpec;

public class DefaultSenseiZoieIndexable<V> extends AbstractZoieIndexable{

	private static final Logger logger = Logger.getLogger(DefaultSenseiZoieIndexable.class);
	
	private final V _obj;
	private final DefaultSenseiInterpreter<V> _interpreter;
	
	DefaultSenseiZoieIndexable(V obj,DefaultSenseiInterpreter<V> interpreter){
		_obj = obj;
		_interpreter = interpreter;
	}
	
	@Override
	public IndexingReq[] buildIndexingReqs() {
		Document doc = new Document();
		Set<Entry<String,IndexSpec>> entries =  _interpreter._textIndexingSpecMap.entrySet();
		for (Entry<String,IndexSpec> entry : entries){
		  try{
		    String name = entry.getKey();
		    IndexSpec idxSpec = entry.getValue();
		    String val = String.valueOf(idxSpec.fld.get(_obj));
		    doc.add(new org.apache.lucene.document.Field(name,val,idxSpec.store,idxSpec.index,idxSpec.tv));
		  }
		  catch(Exception e){
			logger.error(e.getMessage(),e);
		  }
		}
		Set<Entry<String,MetaFormatSpec>> metaEntries = _interpreter._metaFormatSpecMap.entrySet();
		for (Entry<String,MetaFormatSpec> entry : metaEntries){

		    String name = entry.getKey();
		  try{

			MetaFormatSpec formatSpec = entry.getValue();
			    
			Object valObj = formatSpec.fld.get(_obj);
			if (valObj == null) continue;
		    Format formatter = formatSpec.formatter;
		    
		    Collection valueCollection = null;
		    if (valObj instanceof Collection){
		    	valueCollection = (Collection)valObj;
		    }
		    else{
		    	valueCollection = new LinkedList();
		    	valueCollection.add(valObj);
		    }
		    
		    for (Object obj : valueCollection){
		      String val = formatter==null ? String.valueOf(obj) : formatter.format(obj);
		    
		      org.apache.lucene.document.Field fld = new org.apache.lucene.document.Field(name,val,Store.NO,Index.NOT_ANALYZED_NO_NORMS,TermVector.NO);
		      fld.setOmitTermFreqAndPositions(true);
		      doc.add(fld);
		    }
		  }
		  catch(Exception e){
			logger.error("error constructing indexable for field: "+name+" ==> "+e.getMessage(),e);
		  }
		}
		return new IndexingReq[]{new IndexingReq(doc)};
	}

  //@Override
  //public byte[] getStoreValue()
  //{
    //throw new NotImplementedException();
  //}

  @Override
	public long getUID() {
		try {
			return _interpreter._uidField.getLong(_obj);
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage(),e);
		}
	}
	
	private boolean checkViaReflection(Method m){
		boolean retVal = false;
		if (m!=null){
			try {
				Object retObj = m.invoke(_obj, new Object[0]);
				retVal = ((Boolean)retObj).booleanValue();
			} catch (Exception e) {
				throw new RuntimeException(e.getMessage(),e);
			}
		}
		return retVal;
	}

	@Override
	public boolean isDeleted() {
		return checkViaReflection(_interpreter._deleteChecker);
	}

	@Override
	public boolean isSkip() {

		return checkViaReflection(_interpreter._skipChecker);
	}

	
}
