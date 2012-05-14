package com.senseidb.search.req;

import com.linkedin.bobo.api.BrowseHit;

public class SenseiHit extends BrowseHit
{
  private static final long serialVersionUID = 1L;
  
  private long _uid = Long.MIN_VALUE;
  private String _srcData = "";
  private byte[] _storedValue = null;
  
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
}
