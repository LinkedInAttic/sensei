package com.senseidb.search.node.broker;

import java.util.List;

import com.senseidb.search.req.SenseiRequest;

public class AllClustersPruner implements LayeredClusterPruner {

  @Override
  public List<String> pruneClusters(SenseiRequest request, List<String> clusters) {    
    return clusters;
  }

  @Override
  public boolean clusterPrioritiesEqual(SenseiRequest request) {   
    return false;
  }

}
