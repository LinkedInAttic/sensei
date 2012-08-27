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

import org.apache.commons.codec.binary.Base64;
import org.json.JSONObject;

public abstract class DataSourceFilter<D>
{
  protected String _srcDataStore;
  protected String _srcDataField = "src_data";

  protected abstract JSONObject doFilter(D data) throws Exception;

  public JSONObject filter(D data) throws Exception
  {
    JSONObject obj = doFilter(data);
    if (data != null && obj != null && !obj.has(_srcDataField) && _srcDataStore != null && _srcDataStore.length() != 0 &&
        !"none".equals(_srcDataStore) && _srcDataField != null && _srcDataField.length() != 0)
    {
      if (data instanceof byte[])
      {
        obj.put(_srcDataField, Base64.encodeBase64String((byte[])data));
      }
      else
      {
        obj.put(_srcDataField, data.toString());
      }
    }
    return obj;
  }

  public void setSrcDataStore(String srcDataStore)
  {
    _srcDataStore = srcDataStore;
  }

  public void setSrcDataField(String srcDataField)
  {
    _srcDataField = srcDataField;
  }
}

