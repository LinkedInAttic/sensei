package com.sensei.search.req;

import com.browseengine.bobo.api.BrowseHit;

public class SenseiHit extends BrowseHit
{
  private static final long serialVersionUID = 1L;
  
  private long _uid = Long.MIN_VALUE;
  
  public void setUID(long uid)
  {
    _uid = uid;
  }
  
  public long getUID()
  {
    return _uid;
  }
}
