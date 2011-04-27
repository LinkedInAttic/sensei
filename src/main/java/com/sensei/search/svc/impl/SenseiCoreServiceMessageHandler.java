package com.sensei.search.svc.impl;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.Message;
import com.linkedin.norbert.javacompat.network.MessageHandler;
import com.sensei.search.req.AbstractSenseiRequest;
import com.sensei.search.req.AbstractSenseiResult;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.TimerMetric;

public final class SenseiCoreServiceMessageHandler implements MessageHandler {
    private final AbstractSenseiCoreService<AbstractSenseiRequest, AbstractSenseiResult> _svc;

    private final TimerMetric searchTimer = Metrics.newTimer(SenseiCoreServiceMessageHandler.class, "sensei-search-time-all", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
    private final TimerMetric searchExecutionTimer = Metrics.newTimer(SenseiCoreServiceMessageHandler.class, "sensei-search-time-execute", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
    
    public SenseiCoreServiceMessageHandler(AbstractSenseiCoreService<AbstractSenseiRequest, AbstractSenseiResult> svc){
		_svc = svc;
	}
	
	@Override
	public Message handleMessage(final Message msg) throws Exception {
		return searchTimer.time(new Callable<Message>(){

			@Override
			public Message call() throws Exception {
				final AbstractSenseiRequest req = _svc.reqFromMessage(msg);
				
				AbstractSenseiResult res = searchExecutionTimer.time(new Callable<AbstractSenseiResult>(){

					@Override
					public AbstractSenseiResult call() throws Exception {
						return _svc.execute(req);
					}
					
				});
				return _svc.resultToMessage(res);
			}
			
		});
		
	}

}
