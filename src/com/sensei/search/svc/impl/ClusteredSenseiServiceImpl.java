package com.sensei.search.svc.impl;

import org.apache.log4j.Logger;

import com.linkedin.norbert.cluster.ClusterShutdownException;
import com.linkedin.norbert.javacompat.cluster.ClusterClient;
import com.sensei.search.nodes.SenseiBroker;
import com.sensei.search.req.SenseiRequest;
import com.sensei.search.req.SenseiResult;
import com.sensei.search.svc.api.SenseiException;
import com.sensei.search.svc.api.SenseiService;

public class ClusteredSenseiServiceImpl implements SenseiService {
	private static Logger log = Logger.getLogger(ClusteredSenseiServiceImpl.class);

	private SenseiBroker _broker;
	private ClusterClient _cluster;
	
	public SenseiResult doQuery(SenseiRequest req) throws SenseiException {
		return _broker.browse(req);
	}
	
	public void shutdown(){
		try{
		  log.info("shutting down client...");
		  _broker.shutdown();
		}
		finally{
		  log.info("shutting down cluster...");
          try{
            _cluster.shutdown();
          } catch (ClusterShutdownException e) {
        	  log.error(e.getMessage(),e);
		}
          finally{
          }
          
		}
	}

}
