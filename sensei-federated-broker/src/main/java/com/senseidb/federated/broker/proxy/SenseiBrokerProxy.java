/**
 * This software is licensed to you under the Apache License, Version 2.0 (the
 * "Apache License").
 *
 * LinkedIn's contributions are made under the Apache License. If you contribute
 * to the Software, the contributions will be deemed to have been made under the
 * Apache License, unless you expressly indicate otherwise. Please do not make any
 * contributions that would be inconsistent with the Apache License.
 *
 * You may obtain a copy of the Apache License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, this software
 * distributed under the Apache License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the Apache
 * License for the specific language governing permissions and limitations for the
 * software governed under the Apache License.
 *
 * © 2012 LinkedIn Corp. All Rights Reserved.  
 */

package com.senseidb.federated.broker.proxy;

import com.senseidb.metrics.MetricFactory;
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
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Timer;

public class SenseiBrokerProxy extends SenseiBroker implements BrokerProxy {
  private final static Logger logger = Logger.getLogger(SenseiBrokerProxy.class);
  private static PartitionedLoadBalancerFactory balancerFactory = new SenseiPartitionedLoadBalancerFactory(50);  
  private final Timer scatterTimer;
  private final Meter ErrorMeter;

  public SenseiBrokerProxy(PartitionedNetworkClient<String> networkClient, ClusterClient clusterClient, boolean allowPartialMerge) {
    super(networkClient, clusterClient, allowPartialMerge);

    MetricName scatterMetricName = new MetricName(SenseiBrokerProxy.class,"scatter-time");
    scatterTimer = MetricFactory.newTimer(scatterMetricName, TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
    MetricName errorMetricName = new MetricName(SenseiBrokerProxy.class,"error-meter");
    ErrorMeter = MetricFactory.newMeter(errorMetricName, "errors",TimeUnit.SECONDS);
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
