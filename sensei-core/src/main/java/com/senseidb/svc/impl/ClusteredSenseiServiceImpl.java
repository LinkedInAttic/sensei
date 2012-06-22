package com.senseidb.svc.impl;

import java.util.Comparator;

import com.linkedin.norbert.javacompat.network.PartitionedLoadBalancerFactory;

import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;

import com.linkedin.norbert.javacompat.cluster.ClusterClient;
import com.linkedin.norbert.javacompat.cluster.ZooKeeperClusterClient;
import com.linkedin.norbert.javacompat.network.NetworkClientConfig;
import com.senseidb.cluster.client.SenseiNetworkClient;
import com.senseidb.search.node.SenseiBroker;
import com.senseidb.search.node.SenseiSysBroker;
import com.senseidb.search.node.broker.BrokerConfig;
import com.senseidb.search.req.SenseiRequest;
import com.senseidb.search.req.SenseiResult;
import com.senseidb.search.req.SenseiSystemInfo;
import com.senseidb.svc.api.SenseiException;
import com.senseidb.svc.api.SenseiService;

public class ClusteredSenseiServiceImpl implements SenseiService {  
  private static final Logger logger = Logger.getLogger(ClusteredSenseiServiceImpl.class);

  private final NetworkClientConfig _networkClientConfig = new NetworkClientConfig();
  
  private SenseiBroker _senseiBroker;
  private SenseiSysBroker _senseiSysBroker;
  private SenseiNetworkClient _networkClient = null;
  private ClusterClient _clusterClient;
  private final String _clusterName;
  
  public ClusteredSenseiServiceImpl(Configuration senseiConf, PartitionedLoadBalancerFactory<String> loadBalancerFactory/*, SenseiLoadBalancerFactory loadBalancerFactory*/,
      Comparator<String> versionComparator) {
    BrokerConfig brokerConfig = new BrokerConfig(senseiConf, loadBalancerFactory);
    brokerConfig.init();
    _clusterName = brokerConfig.getClusterName();
  
    
    _clusterClient = brokerConfig.getClusterClient();
  
    
    _networkClient = brokerConfig.getNetworkClient();
    _senseiBroker = brokerConfig.buildSenseiBroker();
    _senseiSysBroker = brokerConfig.buildSysSenseiBroker(versionComparator);
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
    return _senseiSysBroker.browse(new SenseiRequest());
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
        try
        {
          if (_senseiSysBroker!=null){
            _senseiSysBroker.shutdown();
            _senseiSysBroker = null;
          }
        }
        finally
        {
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

}
