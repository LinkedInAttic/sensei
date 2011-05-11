package com.sensei.search.nodes;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import com.linkedin.norbert.javacompat.cluster.Node;
import com.sensei.search.req.protobuf.SenseiRequestSerializer;
import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;

import com.browseengine.bobo.api.FacetSpec;
import com.linkedin.norbert.NorbertException;
import com.linkedin.norbert.javacompat.cluster.ClusterClient;
import com.linkedin.norbert.javacompat.network.PartitionedNetworkClient;
import com.linkedin.norbert.javacompat.network.PartitionedLoadBalancerFactory;
import com.sensei.conf.SenseiSchema;
import com.sensei.search.client.ResultMerger;
import com.sensei.search.req.SenseiHit;
import com.sensei.search.req.SenseiRequest;
import com.sensei.search.req.SenseiResult;
import com.sensei.search.req.protobuf.SenseiRequestBPO.Request;

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

  public SenseiBroker(PartitionedNetworkClient<Integer> networkClient, ClusterClient clusterClient, PartitionedLoadBalancerFactory<Integer> loadBalancerFactory) throws NorbertException
  {
    super(networkClient, clusterClient, loadBalancerFactory, SenseiRequestSerializer.getInstance());
    logger.info("created broker instance " + networkClient + " " + clusterClient + " " + loadBalancerFactory);
  }

  @Override
  public SenseiResult mergeResults(SenseiRequest request, List<SenseiResult> resultList)
  {
    request.restoreOrigFacetMaxCounts();
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
      }
    }

    return res;
  }

  public SenseiResult getEmptyResultInstance()
  {
    return new SenseiResult();
  }

  @Override
  public void setTimeoutMillis(long timeoutMillis){
    _timeoutMillis = timeoutMillis;
  }

  @Override
  public long getTimeoutMillis(){
    return _timeoutMillis;
  }


}
