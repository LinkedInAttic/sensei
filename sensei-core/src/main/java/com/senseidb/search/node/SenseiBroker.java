package com.senseidb.search.node;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;

import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;

import proj.zoie.api.indexing.AbstractZoieIndexable;

import com.browseengine.bobo.api.FacetSpec;
import com.linkedin.norbert.NorbertException;
import com.linkedin.norbert.javacompat.cluster.ClusterClient;
import com.linkedin.norbert.javacompat.cluster.Node;
import com.linkedin.norbert.javacompat.network.PartitionedNetworkClient;
import com.senseidb.conf.SenseiSchema;
import com.senseidb.indexing.DefaultJsonSchemaInterpreter;
import com.senseidb.search.req.ErrorType;
import com.senseidb.search.req.SenseiError;
import com.senseidb.search.req.SenseiHit;
import com.senseidb.search.req.SenseiRequest;
import com.senseidb.search.req.SenseiResult;
import com.senseidb.svc.impl.CoreSenseiServiceImpl;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;


/**
 * This SenseiBroker routes search(browse) request using the routers created by
 * the supplied router factory. It uses Norbert's scatter-gather handling
 * mechanism to handle distributed search, which does not support request based
 * context sensitive routing.
 */
public class SenseiBroker extends AbstractConsistentHashBroker<SenseiRequest, SenseiResult> implements SenseiBrokerMBean
{
  private final static Logger logger = Logger.getLogger(SenseiBroker.class);
  private final static long TIMEOUT_MILLIS = 8000L;
  private long _timeoutMillis = TIMEOUT_MILLIS;
  private final boolean allowPartialMerge;
  private final ClusterClient clusterClient;
  private static Counter numberOfNodesInTheCluster = Metrics.newCounter(new MetricName(SenseiBroker.class, "numberOfNodesInTheCluster"));
  public SenseiBroker(PartitionedNetworkClient<String> networkClient, ClusterClient clusterClient, boolean allowPartialMerge) throws NorbertException
  {
    super(networkClient, CoreSenseiServiceImpl.JAVA_SERIALIZER);
	this.clusterClient = clusterClient;
    this.allowPartialMerge = allowPartialMerge;
    MBeanServer platformMBeanServer = ManagementFactory.getPlatformMBeanServer();
	ObjectName name;
	try {
		name = new ObjectName("com.senseidb.search.node:type=SenseiBroker");
		platformMBeanServer.registerMBean(this, name);
	    
	} catch (Exception e) {
		throw new RuntimeException(e.getMessage(), e);
	}	
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

	@Override
	public int getNumberOfNodes() {
		return clusterClient.getNodes().size();
	}
  
	
	
	@Override
	public String getNodeStatistics() {
		StringBuilder str = new StringBuilder("Nodes:");
		for (Node node : clusterClient.getNodes()) {
			str.append("[nodeId=" + node.getId() + ", partitions=["	+ node.getPartitionIds() + "]],");
		}
		return str.toString();
	}
}
