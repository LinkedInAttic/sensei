package com.sensei.search.nodes;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;

import com.browseengine.bobo.api.FacetSpec;
import com.linkedin.norbert.NorbertException;
import com.linkedin.norbert.network.Serializer;
import com.linkedin.norbert.javacompat.cluster.ClusterClient;
import com.linkedin.norbert.javacompat.cluster.Node;
import com.linkedin.norbert.javacompat.network.PartitionedNetworkClient;
import com.sensei.conf.SenseiSchema;
import com.sensei.search.client.ResultMerger;
import com.sensei.search.cluster.routing.SenseiLoadBalancerFactory;
import com.sensei.search.req.SenseiHit;
import com.sensei.search.req.SenseiRequest;
import com.sensei.search.req.SenseiResult;
import com.sensei.search.svc.api.SenseiException;
import com.sensei.search.svc.impl.CoreSenseiServiceImpl;
import com.sensei.search.svc.impl.HttpRestSenseiServiceImpl;

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

  public SenseiBroker(PartitionedNetworkClient<Integer> networkClient, ClusterClient clusterClient,
                      SenseiLoadBalancerFactory loadBalancerFactory) throws NorbertException
  {
    super(networkClient, CoreSenseiServiceImpl.PROTO_SERIALIZER); //TODO: Switch to the java serializer after upgrade
    _loadBalancerFactory = loadBalancerFactory;
    clusterClient.addListener(this);
    logger.info("created broker instance " + networkClient + " " + clusterClient + " " + loadBalancerFactory);
  }

  private void recoverSrcData(SenseiHit[] hits)
  {
    if (hits != null)
    {
      for(SenseiHit hit : hits)
      {
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
            dataBytes = doc.getBinaryValue(SenseiSchema.SRC_DATA_FIELD_NAME);
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
        recoverSrcData(hit.getSenseiGroupHits());
      }
    }
  }

  @Override
  public SenseiResult mergeResults(SenseiRequest request, List<SenseiResult> resultList)
  {
    request.restoreState();
    SenseiResult res = ResultMerger.merge(request, resultList, false);

    if (request.isFetchStoredFields()) {  // Decompress binary data.
      recoverSrcData(res.getSenseiHits());
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
  public SenseiResult browse(SenseiRequest req) throws SenseiException {
	  try{
	    List<NameValuePair> queryParams = HttpRestSenseiServiceImpl.convertRequestToQueryParams(req);
	    String qString = URLEncodedUtils.format(queryParams, "UTF-8");
		Logger log = Logger.getLogger("com.sensei.querylog");
		log.info(qString);
	  }
	  catch(Exception e){
		logger.error(e.getMessage(),e);
	  }
	  return super.browse(req);
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
        if (spec != null)
          spec.setMaxCount(50);
      }
    }

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
