package com.sensei.search.svc.impl;

import com.linkedin.norbert.javacompat.network.RequestHandler;
import com.sensei.search.req.AbstractSenseiRequest;
import com.sensei.search.req.AbstractSenseiResult;

public final class SenseiCoreServiceMessageHandler<Req extends AbstractSenseiRequest, Res extends AbstractSenseiResult>
        implements RequestHandler<Req, Res> {

    private final AbstractSenseiCoreService<Req, Res> _svc;

    public SenseiCoreServiceMessageHandler(AbstractSenseiCoreService<Req, Res> svc){
		_svc = svc;
	}

  @Override
  public Res handleRequest(Req req) throws Exception {
    return _svc.execute(req);
  }
}
