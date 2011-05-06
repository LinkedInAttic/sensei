package com.sensei.search.svc.impl;

import java.util.Comparator;

import org.apache.log4j.Logger;

import com.linkedin.norbert.javacompat.cluster.ClusterClient;
import com.linkedin.norbert.javacompat.cluster.ZooKeeperClusterClient;
import com.linkedin.norbert.javacompat.network.NetworkClientConfig;
import com.sensei.search.cluster.client.SenseiNetworkClient;
import com.sensei.search.cluster.routing.SenseiLoadBalancerFactory;
import com.sensei.search.nodes.SenseiBroker;
import com.sensei.search.nodes.impl.NoopRequestScatterRewriter;
import com.sensei.search.req.SenseiSystemInfo;
import com.sensei.search.req.SenseiRequest;
import com.sensei.search.req.SenseiResult;
import com.sensei.search.svc.api.SenseiException;
import com.sensei.search.svc.api.SenseiService;

public class ClusteredSenseiServiceImpl implements SenseiService {  
  private static final Logger logger = Logger.getLogger(ClusteredSenseiServiceImpl.class);

  private final NoopRequestScatterRewriter _reqRewriter = new NoopRequestScatterRewriter();
  private final NetworkClientConfig _networkClientConfig = new NetworkClientConfig();
  
  private SenseiBroker _senseiBroker;
  private SenseiNetworkClient _networkClient = null;
  private ClusterClient _clusterClient;
  private final String _clusterName;
  
  public ClusteredSenseiServiceImpl(String zkurl,int zkTimeout,String clusterName, int connectTimeoutMillis,
      int writeTimeoutMillis, int maxConnectionsPerNode, int staleRequestTimeoutMins,
      int staleRequestCleanupFrequencyMins, SenseiLoadBalancerFactory loadBalancerFactory,
      Comparator<String> versionComparator) {
    _clusterName = clusterName;
    _networkClientConfig.setServiceName(clusterName);
    _networkClientConfig.setZooKeeperConnectString(zkurl);
    _networkClientConfig.setZooKeeperSessionTimeoutMillis(zkTimeout);
    _networkClientConfig.setConnectTimeoutMillis(connectTimeoutMillis);
    _networkClientConfig.setWriteTimeoutMillis(writeTimeoutMillis);
    _networkClientConfig.setMaxConnectionsPerNode(maxConnectionsPerNode);
    _networkClientConfig.setStaleRequestTimeoutMins(staleRequestTimeoutMins);
    _networkClientConfig.setStaleRequestCleanupFrequencyMins(staleRequestCleanupFrequencyMins);
    
    _clusterClient = new ZooKeeperClusterClient(clusterName,zkurl,zkTimeout);
  
    _networkClientConfig.setClusterClient(_clusterClient);
    
    _networkClient = new SenseiNetworkClient(_networkClientConfig,null);
    _senseiBroker = new SenseiBroker(_networkClient, _clusterClient, _reqRewriter, loadBalancerFactory, versionComparator);
  }
  
  public void start(){
    logger.info("Connecting to cluster: "+_clusterName+" ...");
    _clusterClient.awaitConnectionUninterruptibly();

    logger.info("Cluster: "+_clusterName+" successfully connected ");
  }
  
  public SenseiResult doQuery(SenseiRequest req) throws SenseiException {
    return _senseiBroker.browse(req);
  }
  
  
  @Override
  public SenseiSystemInfo getSystemInfo() throws SenseiException {
    return null; //TODO:wonlay: _senseiBroker.getSystemInfo();
  }

  @Override
  public void shutdown(){
    try{
        if (_senseiBroker!=null){
          _senseiBroker.shutdown();
          _senseiBroker = null;
        }
      }
      finally{
        try{
          if (_networkClient!=null){
            _networkClient.shutdown();
            _networkClient = null;
          }
        }
        finally{
          if (_clusterClient!=null){
            _clusterClient.shutdown();
            _clusterClient = null;
          }
        }
      }
  }

}
