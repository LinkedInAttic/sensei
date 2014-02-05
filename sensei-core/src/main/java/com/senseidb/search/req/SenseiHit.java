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
package com.senseidb.search.req;

import com.browseengine.bobo.api.BrowseHit;

public class SenseiHit extends BrowseHit
{
  private static final long serialVersionUID = 1L;
  
  private long _uid = Long.MIN_VALUE;
  private String _srcData = "";
  private byte[] _storedValue = null;
  private float[] _features = null;
  
  public SenseiHit[] getSenseiGroupHits()
  {
    BrowseHit[] hits = getGroupHits();
    if (hits == null || hits.length == 0)
    {
      return new SenseiHit[0];
    }
    return (SenseiHit[]) hits;
  }

  public void setUID(long uid)
  {
    _uid = uid;
  }
  
  public long getUID()
  {
    return _uid;
  }

  public void setSrcData(String data)
  {
    _srcData = data;
  }

  public String getSrcData()
  {
    return _srcData;
  }

  public void setStoredValue(byte[] value)
  {
    _storedValue = value;
  }

  public byte[] getStoredValue()
  {
    return _storedValue;
  }

  public float[] getFeatures()
  {
    return _features;
  }

  public void setFeatures(float[] features)
  {
    this._features = features;
  }
}
