package com.senseidb.svc.api;

import com.senseidb.search.req.SenseiRequest;
import com.senseidb.search.req.SenseiResult;
import com.senseidb.search.req.SenseiSystemInfo;

public interface SenseiService {
	SenseiResult doQuery(SenseiRequest req) throws SenseiException;
	SenseiSystemInfo getSystemInfo() throws SenseiException;
	void shutdown();
}
