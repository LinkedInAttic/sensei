package com.sensei.search.svc.impl;

import java.io.File;

import org.apache.log4j.Logger;

import com.linkedin.norbert.NorbertException;
import com.linkedin.norbert.cluster.ClusterShutdownException;
import com.linkedin.norbert.cluster.javaapi.ClusterClient;
import com.linkedin.norbert.network.javaapi.PartitionedLoadBalancerFactory;
import com.linkedin.norbert.network.javaapi.PartitionedNetworkClient;
import com.sensei.search.cluster.client.SenseiNetworkClient;
import com.sensei.search.cluster.routing.UniformPartitionedRoutingFactory;
import com.sensei.search.nodes.SenseiBroker;
import com.sensei.search.nodes.SenseiRequestScatterRewriter;
import com.sensei.search.req.SenseiRequest;
import com.sensei.search.req.SenseiResult;
import com.sensei.search.req.protobuf.SenseiRequestBPO;
import com.sensei.search.req.protobuf.SenseiResultBPO;
import com.sensei.search.svc.api.SenseiException;
import com.sensei.search.svc.api.SenseiService;
import com.sensei.search.util.SenseiDefaults;

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
