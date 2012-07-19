package com.senseidb.federated.broker.proxy;

import java.util.List;

import com.senseidb.search.req.SenseiRequest;
import com.senseidb.search.req.SenseiResult;

public interface BrokerProxy {
   List<SenseiResult> doQuery(SenseiRequest senseiRequest);
}
