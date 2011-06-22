package com.sensei.search.svc.impl;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import javax.management.ObjectName;

import org.apache.log4j.Logger;

import com.google.protobuf.Message;
import com.linkedin.norbert.javacompat.network.MessageHandler;
import com.sensei.search.jmx.JmxUtil;
import com.sensei.search.jmx.JmxUtil.Timer;
import com.sensei.search.req.AbstractSenseiRequest;
import com.sensei.search.req.AbstractSenseiResult;
import com.yammer.metrics.core.TimerMetric;

public final class SenseiCoreServiceMessageHandler implements MessageHandler {
	private static final Logger logger = Logger.getLogger(SenseiCoreServiceMessageHandler.class);
    private final AbstractSenseiCoreService<AbstractSenseiRequest, AbstractSenseiResult> _svc;


    private final static TimerMetric TotalSearchTimer = new TimerMetric(TimeUnit.MILLISECONDS,TimeUnit.SECONDS);
    static{
  	  // register jmx monitoring for timers
  	  try{
  	    ObjectName totalMBeanName = new ObjectName(JmxUtil.Domain+".node","name","total-search-time");
  	    JmxUtil.registerMBean(TotalSearchTimer, totalMBeanName);
  	 }
	  catch(Exception e){
		logger.error(e.getMessage(),e);
	  }
   }
    public SenseiCoreServiceMessageHandler(AbstractSenseiCoreService<AbstractSenseiRequest, AbstractSenseiResult> svc){
		_svc = svc;
	}
	
	@Override
	public Message handleMessage(final Message msg) throws Exception {
		return TotalSearchTimer.time(new Callable<Message>(){

			@Override
			public Message call() throws Exception {
				final AbstractSenseiRequest req = _svc.reqFromMessage(msg);
				
				AbstractSenseiResult res = _svc.execute(req);
				return _svc.resultToMessage(res);
			}
		});
	}

}
