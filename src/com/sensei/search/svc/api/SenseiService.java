package com.sensei.search.svc.api;

import com.sensei.search.req.SenseiSystemInfo;
import com.sensei.search.req.SenseiRequest;
import com.sensei.search.req.SenseiResult;

public interface SenseiService {
	SenseiResult doQuery(SenseiRequest req) throws SenseiException;
	SenseiSystemInfo getSystemInfo() throws SenseiException;
}
