package com.sensei.search.nodes;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

import com.linkedin.norbert.NorbertException;
import com.linkedin.norbert.javacompat.cluster.ClusterClient;
import com.linkedin.norbert.javacompat.cluster.Node;
import com.linkedin.norbert.javacompat.network.PartitionedLoadBalancerFactory;
import com.linkedin.norbert.javacompat.network.PartitionedNetworkClient;
import com.sensei.conf.SenseiSchema;
import com.sensei.search.client.ResultMerger;
import com.sensei.search.cluster.routing.SenseiLoadBalancerFactory;
import com.sensei.search.req.SenseiHit;
import com.sensei.search.req.SenseiRequest;
import com.sensei.search.req.SenseiResult;
import com.sensei.search.req.SenseiSystemInfo;
import com.sensei.search.req.protobuf.SenseiRequestBPO;
import com.sensei.search.req.protobuf.SenseiRequestBPOConverter;
import com.sensei.search.req.protobuf.SenseiResultBPO;
import com.sensei.search.req.protobuf.SenseiSysRequestBPO;
import com.sensei.search.req.protobuf.SenseiSysRequestBPOConverter;
import com.sensei.search.req.protobuf.SenseiSysResultBPO;
import com.sensei.search.req.protobuf.SenseiRequestBPO.Request;
import com.sensei.search.req.protobuf.SenseiResultBPO.Result;
import com.sensei.search.svc.api.SenseiException;

/**
 * This SenseiBroker routes search(browse) request using the routers created by
 * the supplied router factory. It uses Norbert's scatter-gather handling
 * mechanism to handle distributed search, which does not support request based
 * context sensitive routing.
 */
public class SenseiBroker extends AbstractConsistentHashBroker<SenseiRequest, SenseiResult, SenseiRequestBPO.Request, SenseiResultBPO.Result>
{
  private final static Logger logger = Logger.getLogger(SenseiBroker.class);
  private final static long TIMEOUT_MILLIS = 8000L;

  private long _timeoutMillis = TIMEOUT_MILLIS;

  public SenseiBroker(PartitionedNetworkClient<Integer> networkClient, ClusterClient clusterClient, SenseiRequestScatterRewriter reqRewriter,
      SenseiLoadBalancerFactory loadBalancerFactory, Comparator<String> versionComparator) throws NorbertException
  {
    super(networkClient, clusterClient, SenseiRequestBPO.Request.getDefaultInstance(), SenseiResultBPO.Result.getDefaultInstance(),loadBalancerFactory);
    logger.info("created broker instance " + networkClient + " " + clusterClient + " " + loadBalancerFactory + " " + reqRewriter);
  }

  @Override
  public SenseiResult mergeResults(SenseiRequest request, List<SenseiResult> resultList)
  {
    SenseiResult res = ResultMerger.merge(request, resultList, false);

    if (request.isFetchStoredFields()) {  // Decompress binary data.
      for(SenseiHit hit : res.getSenseiHits()) {
        try
        {
          Document doc = hit.getStoredFields();
          byte[] dataBytes = doc.getBinaryValue(SenseiSchema.SRC_DATA_COMPRESSED_FIELD_NAME);
          if (dataBytes!=null && dataBytes.length>0){
            ByteArrayOutputStream bout = new ByteArrayOutputStream();
            byte[] buf = new byte[1024];  // 1k buffer
            ByteArrayInputStream bin = new ByteArrayInputStream(dataBytes);
            GZIPInputStream gzipStream = new GZIPInputStream(bin);

            int len;
            while ((len = gzipStream.read(buf)) > 0) {
              bout.write(buf, 0, len);
            }
            bout.flush();

            byte[] uncompressed = bout.toByteArray();
            hit.setSrcData(new String(uncompressed,"UTF-8"));
          }
          else {
            dataBytes = doc.getBinaryValue(SenseiSchema.SRC_DATA_COMPRESSED_FIELD_NAME);
            if (dataBytes!=null && dataBytes.length>0) {
              hit.setSrcData(new String(dataBytes,"UTF-8"));
            }
          }
          doc.removeFields(SenseiSchema.SRC_DATA_COMPRESSED_FIELD_NAME);
          doc.removeFields(SenseiSchema.SRC_DATA_FIELD_NAME);
        }
        catch(Exception e)
        {
          logger.error(e.getMessage(),e);
        }
      }
    }

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
  public SenseiResult messageToResult(Result message)
  {
    return SenseiRequestBPOConverter.convert(message);
  }

  @Override
  public Request requestToMessage(SenseiRequest request)
  {
    return SenseiRequestBPOConverter.convert(request);
  }

  @Override
  public void setTimeoutMillis(long timeoutMillis){
    _timeoutMillis = timeoutMillis;
  }

  @Override
  public long getTimeoutMillis(){
    return _timeoutMillis;
  }

  private IntSet getPartitions(Set<Node> nodes)
  {
    IntSet partitionSet = new IntOpenHashSet();
    for (Node n : nodes)
    {
      partitionSet.addAll(n.getPartitionIds());
    }
    return partitionSet;
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
