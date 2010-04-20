package com.sensei.search.svc.impl;

import java.io.File;

import org.apache.log4j.Logger;

import com.linkedin.norbert.NorbertException;
import com.linkedin.norbert.cluster.ClusterShutdownException;
import com.linkedin.norbert.cluster.javaapi.ClusterClient;
import com.linkedin.norbert.network.javaapi.PartitionedLoadBalancerFactory;
import com.linkedin.norbert.network.javaapi.PartitionedNetworkClient;
import com.sensei.search.cluster.client.SenseiClusterClientImpl;
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

    private final String _zkurl;
	private final String _clusterName;
	private final int _zkSessionTimeout;
	private final PartitionedLoadBalancerFactory<Integer> _routerFactory;
	private SenseiBroker _broker;
	private PartitionedNetworkClient<Integer> _networkClient;
	private final SenseiRequestScatterRewriter _reqRewriter;
    private static ClusterClient _cluster = null;
    private boolean _inMemory = false;

	public ClusteredSenseiServiceImpl(String clusterName, String zkurl, int zkTimeout, boolean inMemory) throws SenseiException{
		this(clusterName, zkurl, zkTimeout, new UniformPartitionedRoutingFactory(), null, inMemory);
	}
	
	public ClusteredSenseiServiceImpl(String clusterName, String zkurl, int zkTimeout, SenseiRequestScatterRewriter reqRewriter) 
	throws SenseiException{
		this(clusterName, zkurl, zkTimeout, new UniformPartitionedRoutingFactory(),reqRewriter, false);
	}
	
	public ClusteredSenseiServiceImpl(String clusterName,String zkurl, int zkTimeout, PartitionedLoadBalancerFactory<Integer> routerFactory) 
	throws SenseiException{
		this(clusterName, zkurl, zkTimeout, routerFactory, null, false);
	}
	
	public ClusteredSenseiServiceImpl(String clusterName,String zkurl, int zkTimeout, PartitionedLoadBalancerFactory<Integer> routerFactory, 
	                                  SenseiRequestScatterRewriter reqRewriter, boolean inMemory) throws SenseiException{
		_clusterName = clusterName;
		_zkurl = zkurl;
		_zkSessionTimeout = zkTimeout;
		_routerFactory = routerFactory;
		_reqRewriter = reqRewriter;
		_inMemory = inMemory;
	}
	
	public void setClusterClient(ClusterClient clusterClient)
	{
	  _cluster = clusterClient;
	}

	public ClusterClient getClusterClient()
	{
	  return _cluster;
	}
	
	public void startup() throws SenseiException{
	  if(_cluster == null)
	  {
	    SenseiClusterClientImpl senseiClusterClient = new SenseiClusterClientImpl(_clusterName, _zkurl, _zkSessionTimeout, _inMemory);
	    _cluster = senseiClusterClient.getClusterClient();
	  }
	  
      String confDirName=System.getProperty("conf.dir");
      File confDir = null;
      if (confDirName == null)
      {
        confDir = new File("node-conf");
      }
      else
      {
        confDir = new File(confDirName);
      }

      File confFile = new File(confDir, SenseiDefaults.SENSEI_CLUSTER_CONF_FILE);
            
      _networkClient = new SenseiNetworkClient(confFile, _cluster, _routerFactory);

      // register the request-response messages
      _networkClient.registerRequest(SenseiRequestBPO.Request.getDefaultInstance(), SenseiResultBPO.Result.getDefaultInstance());
      
	  try{
	    _broker = new SenseiBroker(_cluster, _networkClient, _reqRewriter, _routerFactory);
      }
      catch(NorbertException ne){
        log.info("shutting down cluster...");
        try{
          _cluster.shutdown();
        } 
        catch (ClusterShutdownException e) {
          log.info(e.getMessage(), e);  
        }
        finally{
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
