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
package com.senseidb.search.node;


import com.browseengine.bobo.api.FacetSpec;
import com.linkedin.norbert.NorbertException;
import com.linkedin.norbert.javacompat.cluster.ClusterClient;
import com.linkedin.norbert.javacompat.cluster.Node;
import com.linkedin.norbert.javacompat.network.PartitionedNetworkClient;
import com.linkedin.norbert.javacompat.network.RequestBuilder;
import com.linkedin.norbert.network.ResponseIterator;
import com.senseidb.conf.SenseiSchema;
import com.senseidb.indexing.DefaultJsonSchemaInterpreter;
import com.senseidb.metrics.MetricFactory;
import com.senseidb.search.req.ErrorType;
import com.senseidb.search.req.SenseiError;
import com.senseidb.search.req.SenseiHit;
import com.senseidb.search.req.SenseiRequest;
import com.senseidb.search.req.SenseiRequestCustomizer;
import com.senseidb.search.req.SenseiRequestCustomizerFactory;
import com.senseidb.search.req.SenseiResult;
import com.senseidb.svc.impl.CoreSenseiServiceImpl;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import proj.zoie.api.indexing.AbstractZoieIndexable;


/**
 * This SenseiBroker routes search(browse) request using the routers created by
 * the supplied router factory. It uses Norbert's scatter-gather handling
 * mechanism to handle distributed search, which does not support request based
 * context sensitive routing.
 */
public class SenseiBroker extends AbstractConsistentHashBroker<SenseiRequest, SenseiResult> 
{
  private final static Logger logger = Logger.getLogger(SenseiBroker.class);

 
  private final boolean allowPartialMerge;
  private final ClusterClient clusterClient;
  private final SenseiRequestCustomizerFactory requestCustomizerFactory;
  private final Counter numberOfNodesInTheCluster = MetricFactory.newCounter(new MetricName(SenseiBroker.class,
                                                                                            "numberOfNodesInTheCluster"));
  
  public SenseiBroker(PartitionedNetworkClient<String> networkClient,
                      ClusterClient clusterClient,
                      boolean allowPartialMerge,
                      SenseiRequestCustomizerFactory requestCustomizerFactory)
      throws NorbertException {
    super(networkClient, CoreSenseiServiceImpl.JAVA_SERIALIZER);
    this.clusterClient = clusterClient;
    this.allowPartialMerge = allowPartialMerge;
    this.requestCustomizerFactory = requestCustomizerFactory;
    clusterClient.addListener(this);
    logger.info("created broker instance " + networkClient + " " + clusterClient);
  }

  public static void recoverSrcData(SenseiResult res, SenseiHit[] hits, boolean isFetchStoredFields)
  {
    if (hits != null)
    {
      for(SenseiHit hit : hits)
      {
        try
        {
          byte[] dataBytes = hit.getStoredValue();
          if (dataBytes == null || dataBytes.length == 0)
          {
            Document doc = hit.getStoredFields();
            if (doc != null)
            {
              dataBytes = doc.getBinaryValue(AbstractZoieIndexable.DOCUMENT_STORE_FIELD);

              if (dataBytes == null || dataBytes.length == 0)
              {
                dataBytes = doc.getBinaryValue(SenseiSchema.SRC_DATA_COMPRESSED_FIELD_NAME);

                if (dataBytes == null || dataBytes.length == 0)
                {
                  dataBytes = doc.getBinaryValue(SenseiSchema.SRC_DATA_FIELD_NAME);
                  if (dataBytes != null && dataBytes.length > 0)
                  {
                    hit.setSrcData(new String(dataBytes,"UTF-8"));
                    dataBytes = null; // set to null to avoid gunzip.
                  }
                }
                doc.removeFields(SenseiSchema.SRC_DATA_COMPRESSED_FIELD_NAME);
                doc.removeFields(SenseiSchema.SRC_DATA_FIELD_NAME);
              }
            }
          }
          if (dataBytes != null && dataBytes.length > 0)
          {
            byte[] data;
            try
            {
              data = DefaultJsonSchemaInterpreter.decompress(dataBytes);
            }
            catch(Exception ex)
            {
              
              data = dataBytes;
            }
            hit.setSrcData(new String(data, "UTF-8"));
          }
        }
        catch(Exception e)
        {
          logger.error(e.getMessage(),e);
          res.getErrors().add(new SenseiError(e.getMessage(), ErrorType.BrokerGatherError));
        }

        recoverSrcData(res, hit.getSenseiGroupHits(), isFetchStoredFields);

        // Remove stored fields since the user is not requesting:
        if (!isFetchStoredFields)
          hit.setStoredFields(null);
      }
    }
  }

  @Override
  public SenseiResult mergeResults(SenseiRequest request, List<SenseiResult> resultList)
  {
    SenseiResult res = ResultMerger.merge(request, resultList, false);
    
    if (request.isFetchStoredFields() || request.isFetchStoredValue())
      recoverSrcData(res, res.getSenseiHits(), request.isFetchStoredFields());

    return res;
  }

  @Override
  public SenseiResult getEmptyResultInstance()
  {
    return new SenseiResult();
  }

  protected List<SenseiResult> doCall(final SenseiRequest req) throws ExecutionException
  {
    List<SenseiResult> resultList = new ArrayList<SenseiResult>();

    // only instantiate if debug logging is enabled
    final List<StringBuilder> timingLogLines = logger.isDebugEnabled() ? new LinkedList<StringBuilder>() : null;

    final SenseiRequestCustomizer customizer;
    if (requestCustomizerFactory != null)
      customizer = requestCustomizerFactory.getRequestCustomizer(req);
    else
      customizer = null;

    ResponseIterator<SenseiResult> responseIterator =
        buildIterator(_networkClient.sendRequestToOneReplica(getRouteParam(req), new RequestBuilder<Integer, SenseiRequest>() {
          @Override
          public SenseiRequest apply(Node node, Set<Integer> nodePartitions) {
            // TODO: Cloning is yucky per http://www.artima.com/intv/bloch13.html
            SenseiRequest clone = (SenseiRequest) (((SenseiRequest) req).clone());

            clone.setPartitions(nodePartitions);
            if (timingLogLines != null) {
              // this means debug logging was enabled, produce first portion of log lines
              timingLogLines.add(buildLogLineForRequest(node, clone));
            }

            SenseiRequest customizedRequest = customizeRequest(clone, customizer, nodePartitions);
            return customizedRequest;
          }
        }, _serializer));

    while(responseIterator.hasNext()) {
      resultList.add(responseIterator.next());
    }

    if (timingLogLines != null) {
      // this means debug logging was enabled, complete the timing log lines and log them
      int i = 0;
      for (StringBuilder logLine : timingLogLines) {
        // we are assuming the request builder gets called in the same order as the response
        // iterator is built, otherwise the loglines would be out of sync between req & res
        if (i < resultList.size()) {
          logger.debug(buildLogLineForResult(logLine, resultList.get(i++)));
        }
      }
      logger.debug(String.format("There are %d responses", resultList.size()));
    }

    return resultList;
  }
  
  public SenseiRequest customizeRequest(SenseiRequest request, SenseiRequestCustomizer customizer, Set<Integer> nodePartitions)
  {
    // Rewrite offset and count.
    request.setCount(request.getOffset()+request.getCount());
    request.setOffset(0);

    // Rewrite facet max count.
    Map<String, FacetSpec> facetSpecs = request.getFacetSpecs();
    if (facetSpecs != null) {
      for (Map.Entry<String, FacetSpec> entry : facetSpecs.entrySet()) {
        FacetSpec spec = entry.getValue();
        if (spec != null && spec.getMaxCount() < 50)
          spec.setMaxCount(50);
      }
    }

    // Rewrite fetchStoredFields for zoie store.
    if (!request.isFetchStoredFields())
      request.setFetchStoredFields(request.isFetchStoredValue());

    if (customizer != null)
    {
      request = customizer.customize(request, nodePartitions);
    }

    return request;
  }

  @Override
  protected StringBuilder buildLogLineForRequest(Node node, SenseiRequest req) {
    return super.buildLogLineForRequest(node, req).append(" - routeParam: ").append(req.getRouteParam());
  }
  
  @Override
  protected StringBuilder buildLogLineForResult(StringBuilder logLine, SenseiResult result) {
    return super.buildLogLineForResult(logLine, result)
        .append(" - hits: ")
        .append(result.getNumHits())
        .append("/")
        .append(result.getTotalDocs());
  }

  public void handleClusterConnected(Set<Node> nodes)
  {
//    _loadBalancer = _loadBalancerFactory.newLoadBalancer(nodes);
    _partitions = getPartitions(nodes);
    numberOfNodesInTheCluster.clear();
    numberOfNodesInTheCluster.inc(getNumberOfNodes());
    logger.info("handleClusterConnected(): Received the list of nodes from norbert " + nodes.toString());
    logger.info("handleClusterConnected(): Received the list of partitions from router " + _partitions.toString());
  }

  public void handleClusterDisconnected()
  {
    logger.info("handleClusterDisconnected() called");
    _partitions = new IntOpenHashSet();
  }

  public void handleClusterNodesChanged(Set<Node> nodes)
  {

//    _loadBalancer = _loadBalancerFactory.newLoadBalancer(nodes);
    _partitions = getPartitions(nodes);
    numberOfNodesInTheCluster.clear();
    numberOfNodesInTheCluster.inc(getNumberOfNodes());
    logger.info("handleClusterNodesChanged(): Received the list of nodes from norbert " + nodes.toString());
    logger.info("handleClusterNodesChanged(): Received the list of partitions from router " + _partitions.toString());

  }

  @Override
  public void handleClusterShutdown()
  {
    logger.info("handleClusterShutdown() called");
  }

  @Override
  public boolean allowPartialMerge() {
    return allowPartialMerge;
  }


	public int getNumberOfNodes() {
		return clusterClient.getNodes().size();
	}
  
	
}
