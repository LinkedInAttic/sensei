package com.sensei.search.svc.impl;

import com.google.protobuf.Message;
import com.linkedin.norbert.javacompat.network.MessageHandler;
import com.sensei.search.req.AbstractSenseiRequest;
import com.sensei.search.req.AbstractSenseiResult;

public final class SenseiCoreServiceMessageHandler implements MessageHandler {
    private final AbstractSenseiCoreService<AbstractSenseiRequest, AbstractSenseiResult> _svc;

    
    public SenseiCoreServiceMessageHandler(AbstractSenseiCoreService<AbstractSenseiRequest, AbstractSenseiResult> svc){
		_svc = svc;
	}
	
	@Override
	public Message handleMessage(final Message msg) throws Exception {
		final AbstractSenseiRequest req = _svc.reqFromMessage(msg);
		
		AbstractSenseiResult res = _svc.execute(req);
		return _svc.resultToMessage(res);
		
	}

}
