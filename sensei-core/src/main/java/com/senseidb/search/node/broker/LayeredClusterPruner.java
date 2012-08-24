package com.senseidb.search.node.broker;

import java.util.List;

import com.senseidb.search.req.SenseiRequest;

public interface LayeredClusterPruner {
  public List<String> pruneClusters(SenseiRequest request, List<String> clusters);
  /**
   * Returns true if documents in one cluster have not a greater priority(comparison value) than documents in another one. 
   * In this case the broker will treat each cluster as just another partition
   * By default this method should return false
   * @param request
   * @return
   */
  public boolean clusterPrioritiesEqual(SenseiRequest request);
}
