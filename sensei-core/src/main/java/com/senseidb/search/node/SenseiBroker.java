package com.senseidb.search.node;

import com.senseidb.svc.impl.CoreSenseiServiceImpl;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;

import proj.zoie.api.indexing.AbstractZoieIndexable;

import com.browseengine.bobo.api.FacetSpec;
import com.linkedin.norbert.NorbertException;
import com.linkedin.norbert.javacompat.cluster.ClusterClient;
import com.linkedin.norbert.javacompat.cluster.Node;
import com.linkedin.norbert.javacompat.network.PartitionedNetworkClient;
import com.senseidb.cluster.routing.SenseiLoadBalancerFactory;
import com.senseidb.conf.SenseiSchema;
import com.senseidb.indexing.DefaultJsonSchemaInterpreter;
import com.senseidb.search.req.SenseiHit;
import com.senseidb.search.req.SenseiRequest;
import com.senseidb.search.req.SenseiResult;


/**
 * This SenseiBroker routes search(browse) request using the routers created by
 * the supplied router factory. It uses Norbert's scatter-gather handling
 * mechanism to handle distributed search, which does not support request based
 * context sensitive routing.
 */
public class SenseiBroker extends AbstractConsistentHashBroker<SenseiRequest, SenseiResult>
{
  private final static Logger logger = Logger.getLogger(SenseiBroker.class);
  private final static long TIMEOUT_MILLIS = 8000L;
  private long _timeoutMillis = TIMEOUT_MILLIS;
  private final SenseiLoadBalancerFactory _loadBalancerFactory;

  public SenseiBroker(PartitionedNetworkClient<Integer> networkClient,
                      ClusterClient clusterClient,
                      SenseiLoadBalancerFactory loadBalancerFactory,
                      int pollInterval,
                      int minResponses,
                      int maxTotalWait)
    throws NorbertException
  {
    super(networkClient, CoreSenseiServiceImpl.PROTO_SERIALIZER, pollInterval, minResponses, maxTotalWait);
    _loadBalancerFactory = loadBalancerFactory;
    clusterClient.addListener(this);
    logger.info("created broker instance " + networkClient + " " + clusterClient + " " + loadBalancerFactory);
  }

  private void recoverSrcData(SenseiHit[] hits, boolean isFetchStoredFields)
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
        }

        recoverSrcData(hit.getSenseiGroupHits(), isFetchStoredFields);

        // Remove stored fields since the user is not requesting:
        if (!isFetchStoredFields)
          hit.setStoredFields(null);
      }
    }
  }

  @Override
  public SenseiResult mergeResults(SenseiRequest request, List<SenseiResult> resultList)
  {
    request.restoreState();
    SenseiResult res = ResultMerger.merge(request, resultList, false);

    if (request.isFetchStoredFields() || request.isFetchStoredValue())
      recoverSrcData(res.getSenseiHits(), request.isFetchStoredFields());

    return res;
  }

  @Override
  public String getRouteParam(SenseiRequest req)
  {
    return req.getRouteParam();
  }

  @Override
  public SenseiResult getEmptyResultInstance()
  {
    return new SenseiResult();
  }
  
  @Override
  public SenseiRequest customizeRequest(SenseiRequest request)
  {    // Rewrite offset and count.
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

    return request;
  }

  @Override
  public void setTimeoutMillis(long timeoutMillis){
    _timeoutMillis = timeoutMillis;
  }

  @Override
  public long getTimeoutMillis(){
    return _timeoutMillis;
  }

  public void handleClusterConnected(Set<Node> nodes)
  {
    _loadBalancer = _loadBalancerFactory.newLoadBalancer(nodes);
    _partitions = getPartitions(nodes);
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
    _loadBalancer = _loadBalancerFactory.newLoadBalancer(nodes);
    _partitions = getPartitions(nodes);
    logger.info("handleClusterNodesChanged(): Received the list of nodes from norbert " + nodes.toString());
    logger.info("handleClusterNodesChanged(): Received the list of partitions from router " + _partitions.toString());
  }

  @Override
  public void handleClusterShutdown()
  {
    logger.info("handleClusterShutdown() called");
  }
}
