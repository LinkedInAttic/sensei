package com.senseidb.search.node;

import com.senseidb.svc.api.SenseiException;

public interface Broker<REQUEST, RESULT> {
  public RESULT browse(final REQUEST req) throws SenseiException;
}
