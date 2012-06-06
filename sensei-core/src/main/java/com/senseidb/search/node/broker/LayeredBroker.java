package com.senseidb.search.node.broker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.linkedin.norbert.javacompat.network.PartitionedLoadBalancerFactory;
import com.senseidb.cluster.routing.SenseiPartitionedLoadBalancerFactory;
import com.senseidb.conf.SenseiConfParams;
import com.senseidb.plugin.SenseiPlugin;
import com.senseidb.plugin.SenseiPluginRegistry;
import com.senseidb.search.node.Broker;
import com.senseidb.search.node.ResultMerger;
import com.senseidb.search.node.SenseiBroker;
import com.senseidb.search.req.SenseiRequest;
import com.senseidb.search.req.SenseiResult;
import com.senseidb.svc.api.SenseiException;

public class LayeredBroker implements SenseiPlugin, Broker<SenseiRequest, SenseiResult> {
  private static final String CLUSTERS = "clusters";
  private List<String> clusters = new ArrayList<String>();
  private Map<String, CompoundBrokerConfig> clusterBrokerConfig = new HashMap<String, CompoundBrokerConfig>() ;
  private Map<String, SenseiBroker> brokers = new HashMap<String, SenseiBroker>() ;
  private LayeredClusterPruner federatedPruner;
  @Override
  public void init(Map<String, String> config, SenseiPluginRegistry pluginRegistry) {
    String clustersConfig = config.get(CLUSTERS);
    if (clustersConfig == null) {
      throw new IllegalArgumentException("Clusters param should be present");
    }
    federatedPruner = pluginRegistry.getBeanByFullPrefix(SenseiConfParams.SENSEI_FEDERATED_BROKER_PRUNER, LayeredClusterPruner.class);
    if (federatedPruner == null) {
      federatedPruner = new AllClustersPruner();
    }
    PartitionedLoadBalancerFactory<String> routerFactory = pluginRegistry.getBeanByFullPrefix(SenseiConfParams.SERVER_SEARCH_ROUTER_FACTORY, PartitionedLoadBalancerFactory.class);
    if (routerFactory == null) {
      routerFactory = new SenseiPartitionedLoadBalancerFactory(50);
    }
    for (String cluster : clustersConfig.split(",")) {
      String trimmed = cluster.trim();
      if (trimmed.length() > 0) {
        clusters.add(trimmed);
        clusterBrokerConfig.put(trimmed, new CompoundBrokerConfig(pluginRegistry.getConfiguration(), routerFactory, config, trimmed));
      }
    }    
  }
  
  @Override
  public void start() {
    for (String cluster : clusters) {
      CompoundBrokerConfig brokerConfig = clusterBrokerConfig.get(cluster);
      brokerConfig.init();
      brokers.put(cluster, brokerConfig.buildSenseiBroker());
    }
    
  }

  @Override
  public void stop() {
    for (CompoundBrokerConfig brokerConfig : clusterBrokerConfig.values()) {
      brokerConfig.getSenseiBroker().shutdown();
      brokerConfig.getNetworkClient().shutdown();
      brokerConfig.getClusterClient().shutdown();
    }
  }
  
  public void warmUp() {
    for (SenseiBroker broker : brokers.values()) {
      try {
        broker.browse(new SenseiRequest());
      } catch (SenseiException e) {
        throw new RuntimeException(e);
      }
    }
  }
  public SenseiResult browse(final SenseiRequest req) throws SenseiException {
    List<String> prunedClusters = federatedPruner.pruneClusters(req, clusters);
    int count = req.getCount();
    int offset = req.getOffset();
    if (count == 0) {
      return new SenseiResult();
    }
    
    List<SenseiResult> results = new ArrayList<SenseiResult>();
    if (!federatedPruner.clusterPrioritiesEqual(req)) {
      for (String cluster : prunedClusters) {
        if (count <= 0) {
          break;
        }
        SenseiRequest request = req.clone();
        request.setCount(count);
        request.setOffset(offset);
        SenseiResult currentResult = brokers.get(cluster).browse(request);
        int numHits = currentResult.getNumHits();
        if (offset >= numHits) {
          offset -= numHits;
          continue;
        } else {
          numHits -= offset;
          offset = 0;
          count -= numHits;
          results.add(currentResult);
        }
      }
    } else {
      for (String cluster : prunedClusters) {       
        SenseiRequest request = req.clone();       
        SenseiResult currentResult = brokers.get(cluster).browse(request);      
        results.add(currentResult);
      }
    }
    SenseiResult res = ResultMerger.merge(req, results, false); 
    return res;
  }
  
  
}
