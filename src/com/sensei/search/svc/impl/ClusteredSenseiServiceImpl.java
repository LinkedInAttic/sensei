package com.sensei.search.svc.impl;

import org.apache.log4j.Logger;

import com.google.protobuf.Message;
import com.linkedin.norbert.NorbertException;
import com.linkedin.norbert.cluster.ClusterShutdownException;
import com.linkedin.norbert.cluster.javaapi.Cluster;
import com.linkedin.norbert.cluster.javaapi.RouterFactory;
import com.linkedin.norbert.network.javaapi.ClientBootstrap;
import com.linkedin.norbert.network.javaapi.ClientConfig;
import com.linkedin.norbert.network.javaapi.NetworkClient;
import com.sensei.search.cluster.routing.UniformRoutingFactory;
import com.sensei.search.nodes.SenseiBroker;
import com.sensei.search.nodes.SenseiRequestScatterRewriter;
import com.sensei.search.req.SenseiRequest;
import com.sensei.search.req.SenseiResult;
import com.sensei.search.req.protobuf.SenseiResultBPO;
import com.sensei.search.svc.api.SenseiException;
import com.sensei.search.svc.api.SenseiService;

public class ClusteredSenseiServiceImpl implements SenseiService {
	private static Logger log = Logger.getLogger(ClusteredSenseiServiceImpl.class);
	private final String _zkurl;
	private final RouterFactory _routerFactory;
	private final SenseiBroker _broker;
	private final ClientBootstrap bootstrap;
	private final NetworkClient networkClient;

	public ClusteredSenseiServiceImpl(String clusterName,String zkurl) throws SenseiException{
		this(clusterName,zkurl,new UniformRoutingFactory(),null);
	}
	
	public ClusteredSenseiServiceImpl(String clusterName,String zkurl,SenseiRequestScatterRewriter reqRewriter) throws SenseiException{
		this(clusterName,zkurl,new UniformRoutingFactory(),reqRewriter);
	}
	
	public ClusteredSenseiServiceImpl(String clusterName,String zkurl,RouterFactory routerFactory) throws SenseiException{
		this(clusterName,zkurl,routerFactory,null);
	}
	
	public ClusteredSenseiServiceImpl(String clusterName,String zkurl,RouterFactory routerFactory,SenseiRequestScatterRewriter reqRewriter) throws SenseiException{
		_zkurl = zkurl;
		_routerFactory = routerFactory;
		
		Message[] messages = { SenseiResultBPO.Result.getDefaultInstance() };
		    
		ClientConfig config = new ClientConfig();
	    config.setClusterName(clusterName);
	    config.setZooKeeperUrls(_zkurl);
	    config.setResponseMessages(messages);
	 
	    config.setRouterFactory(_routerFactory);
	    bootstrap = new ClientBootstrap(config);
	    networkClient = bootstrap.getNetworkClient();
	    Cluster cluster = bootstrap.getCluster();
	    try{
		  _broker = new SenseiBroker(cluster,networkClient,reqRewriter);
	    }
	    catch(NorbertException ne){
	      log.info("shutting down bootstrap...");
	      try{
            networkClient.close();
          } 
	      catch (ClusterShutdownException e) {
        	log.info(e.getMessage(), e);  
		  }
          finally{
            bootstrap.shutdown();
          }
	      throw new SenseiException(ne.getMessage(), ne);
	    }
	}
	
	public SenseiResult doQuery(SenseiRequest req) throws SenseiException {
		return _broker.browse(req);
	}
	
	public void shutdown(){
		try{
		  log.info("shutting down client...");
		  _broker.shutdown();
		}
		finally{
		  log.info("shutting down bootstrap...");

          try{
        	  networkClient.close();
          } catch (ClusterShutdownException e) {
        	  log.error(e.getMessage(),e);
		}
          finally{
        	  bootstrap.shutdown();
          }
          
		}
	}

}
