package com.sensei.search.svc.impl;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import javax.management.ObjectName;

import com.linkedin.norbert.javacompat.network.RequestHandler;
import org.apache.log4j.Logger;

import com.sensei.search.jmx.JmxUtil;
import com.sensei.search.jmx.JmxUtil.Timer;
import com.sensei.search.req.AbstractSenseiRequest;
import com.sensei.search.req.AbstractSenseiResult;
import com.yammer.metrics.core.TimerMetric;

public final class SenseiCoreServiceMessageHandler<REQUEST extends AbstractSenseiRequest, RESULT extends AbstractSenseiResult> implements RequestHandler<REQUEST, RESULT> {
	private static final Logger logger = Logger.getLogger(SenseiCoreServiceMessageHandler.class);
    private final AbstractSenseiCoreService<REQUEST, RESULT> _svc;

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
    public SenseiCoreServiceMessageHandler(AbstractSenseiCoreService<REQUEST, RESULT> svc){
		_svc = svc;
	}

    @Override
    public RESULT handleRequest(final REQUEST request) throws Exception {
        return TotalSearchTimer.time(new Callable<RESULT>(){

            @Override
            public RESULT call() throws Exception {
                return _svc.execute(request);
            }
        });
    }
}
