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
package com.senseidb.search.node.impl;

import java.io.UnsupportedEncodingException;

import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;

import com.senseidb.search.node.SenseiQueryBuilder;
import com.senseidb.search.req.SenseiQuery;

public class SimpleQueryBuilder implements SenseiQueryBuilder
{
  protected Query _query = null;
  protected Filter _filter = null;
  
  public SimpleQueryBuilder(SenseiQuery query, QueryParser parser) throws Exception
  {
    doBuild(query, parser);
  }
  
  protected void doBuild(SenseiQuery query, QueryParser parser) throws Exception
  {
    if (query != null)
    {
      byte[] bytes = query.toBytes();
      String qString = null;
      
      try {
        qString = new String(bytes,"UTF-8");
      }
      catch (UnsupportedEncodingException e) {
        throw new ParseException(e.getMessage());
      }
      
      if (qString.length()>0){
    	  synchronized(parser){
            _query = parser.parse(qString);
    	  }
      }
    }
  }
  
  public Query buildQuery() throws ParseException
  {
    return _query;
  }
  
  public Filter buildFilter()
  {
    return _filter;
  }
}
