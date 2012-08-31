package com.senseidb.federated.broker.proxy;

import com.senseidb.search.req.SenseiRequest;

public interface SenseiRequestFactory {
  public SenseiRequest build(SenseiRequest senseiRequest);
}
