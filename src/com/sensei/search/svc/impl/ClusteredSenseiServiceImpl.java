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
import com.sensei.search.cluster.client.ClientBoostrapFactory;
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
	private final String _clusterName;
	private final RouterFactory _routerFactory;
	private SenseiBroker _broker;
	private ClientBootstrap bootstrap;
	private NetworkClient networkClient;
	private final SenseiRequestScatterRewriter _reqRewriter;
	private ClientBoostrapFactory _bootstrapFactory;
	
	private ClientBoostrapFactory _clientBootstrapFactory;

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
		_clusterName = clusterName;
		_zkurl = zkurl;
		_routerFactory = routerFactory;
		_reqRewriter = reqRewriter;
	}
	
	public void setClientBootstrapFactory(ClientBoostrapFactory clientBoostrapFactory){
		_clientBootstrapFactory = clientBoostrapFactory;
	}
	
	public ClientBoostrapFactory getClientBoostrapFactory(){
		return _clientBootstrapFactory == null ? new ClientBoostrapFactory.DefaultClientBoostrapFactory() : _clientBootstrapFactory;
	}
	
	public void startup() throws SenseiException{
		Message[] messages = { SenseiResultBPO.Result.getDefaultInstance() };
	    
		ClientConfig config = new ClientConfig();
	    config.setClusterName(_clusterName);
	    config.setZooKeeperUrls(_zkurl);
	    config.setResponseMessages(messages);
	 
	    config.setRouterFactory(_routerFactory);
	    
	    ClientBoostrapFactory bootstrapFactory = getClientBoostrapFactory();
	    bootstrap = bootstrapFactory.getClientBootstrap(config);
	    networkClient = bootstrap.getNetworkClient();
	    Cluster cluster = bootstrap.getCluster();
	    try{
		  _broker = new SenseiBroker(cluster,networkClient,_reqRewriter);
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
