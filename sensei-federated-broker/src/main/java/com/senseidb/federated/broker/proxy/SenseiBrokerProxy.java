package com.senseidb.federated.broker.proxy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;

import com.linkedin.norbert.javacompat.cluster.ClusterClient;
import com.linkedin.norbert.javacompat.network.PartitionedLoadBalancerFactory;
import com.linkedin.norbert.javacompat.network.PartitionedNetworkClient;
import com.senseidb.cluster.routing.SenseiPartitionedLoadBalancerFactory;
import com.senseidb.metrics.MetricsConstants;
import com.senseidb.search.node.AbstractConsistentHashBroker;
import com.senseidb.search.node.SenseiBroker;
import com.senseidb.search.req.ErrorType;
import com.senseidb.search.req.SenseiError;
import com.senseidb.search.req.SenseiRequest;
import com.senseidb.search.req.SenseiResult;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Timer;

public class SenseiBrokerProxy extends SenseiBroker implements BrokerProxy {
  private final static Logger logger = Logger.getLogger(SenseiBrokerProxy.class);
  private static PartitionedLoadBalancerFactory balancerFactory = new SenseiPartitionedLoadBalancerFactory(50);  
  private static Timer scatterTimer = null; 
  private static Meter ErrorMeter = null;
  static{
    // register metrics monitoring for timers
    try{
      MetricName scatterMetricName = new MetricName(SenseiBrokerProxy.class,"scatter-time");
      scatterTimer = Metrics.newTimer(scatterMetricName, TimeUnit.MILLISECONDS,TimeUnit.SECONDS);      
      MetricName errorMetricName = new MetricName(SenseiBrokerProxy.class,"error-meter");
      ErrorMeter = Metrics.newMeter(errorMetricName, "errors",TimeUnit.SECONDS);
    }
    catch(Exception e){
    logger.error(e.getMessage(),e);
    }
  }
  
  public SenseiBrokerProxy(PartitionedNetworkClient<String> networkClient, ClusterClient clusterClient, boolean allowPartialMerge) {
    super(networkClient, clusterClient, allowPartialMerge);
  }
  public static SenseiBrokerProxy valueOf(Configuration senseiConfiguration, Map<String, String> overrideProperties) {
    BrokerProxyConfig brokerProxyConfig = new BrokerProxyConfig(senseiConfiguration, balancerFactory, overrideProperties);
    brokerProxyConfig.init();
    SenseiBrokerProxy ret = new SenseiBrokerProxy(brokerProxyConfig.getNetworkClient(), brokerProxyConfig.getClusterClient(), true);
    return ret;
  }
  @Override
  public List<SenseiResult> doQuery(final SenseiRequest senseiRequest) {
    final List<SenseiResult> resultList = new ArrayList<SenseiResult>();
    
    try {
      resultList.addAll(scatterTimer.time(new Callable<List<SenseiResult>>() {
        @Override
        public List<SenseiResult> call() throws Exception {
          return doCall(senseiRequest);
        }
      }));
    } catch (Exception e) {
      ErrorMeter.mark();
      SenseiResult emptyResult = getEmptyResultInstance();
      logger.error("Error running scatter/gather", e);
      emptyResult.addError(new SenseiError("Error gathering the results" + e.getMessage(), ErrorType.BrokerGatherError));
      return Arrays.asList(emptyResult);
    }
    return resultList;
  }
}
