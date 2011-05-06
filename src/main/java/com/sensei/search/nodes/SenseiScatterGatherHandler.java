package com.sensei.search.nodes;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

import com.browseengine.bobo.api.FacetSpec;
import com.google.protobuf.Message;
import com.linkedin.norbert.javacompat.cluster.Node;
import com.sensei.conf.SenseiSchema;
import com.sensei.search.client.ResultMerger;
import com.sensei.search.req.SenseiHit;
import com.sensei.search.req.SenseiRequest;
import com.sensei.search.req.SenseiResult;
import com.sensei.search.req.protobuf.SenseiRequestBPO;
import com.sensei.search.req.protobuf.SenseiRequestBPOConverter;
import com.sensei.search.req.protobuf.SenseiResultBPO;
import com.sensei.search.req.protobuf.SenseiRequestBPO.Request;
import com.sensei.search.req.protobuf.SenseiResultBPO.Result;

public class SenseiScatterGatherHandler extends AbstractSenseiScatterGatherHandler<SenseiRequest, SenseiResult, SenseiRequestBPO.Request, SenseiResultBPO.Result> 
{

  private final static Logger logger = Logger.getLogger(SenseiScatterGatherHandler.class);
  
  private final static long TIMEOUT_MILLIS = 8000L;

  private final SenseiRequestScatterRewriter _reqRewriter;
  
  private long _timeoutMillis = TIMEOUT_MILLIS;

  public SenseiScatterGatherHandler(SenseiRequestScatterRewriter reqRewriter)
  {
    _reqRewriter = reqRewriter;
  }
  
  public void setTimeoutMillis(long timeoutMillis){
    _timeoutMillis = timeoutMillis;
  }
  
  public long getTimeoutMillis(){
    return _timeoutMillis;
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

  @Override
  public SenseiRequest messageToRequest(Request msg)
  {
    return SenseiRequestBPOConverter.convert(msg);
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
  public Result resultToMessage(SenseiResult result)
  {
    return SenseiRequestBPOConverter.convert(result);
  }

  @Override
  public Message customizeMessage(Message msg, Node node, Set<Integer> partitions) throws Exception
  {
    SenseiRequestBPO.Request req = (SenseiRequestBPO.Request) msg;
    SenseiRequest senseiReq = SenseiRequestBPOConverter.convert(req);

    // Rewrite facet max count.
    Map<String, FacetSpec> facetSpecs = senseiReq.getFacetSpecs();
    if (facetSpecs != null) {
      for (Map.Entry<String, FacetSpec> entry : facetSpecs.entrySet()) {
        FacetSpec spec = entry.getValue();
        if (spec != null)
          spec.setMaxCount(0);
      }
    }

    int oldOffset = senseiReq.getOffset();
    int oldCount = senseiReq.getCount();
    if (_reqRewriter != null)
    {
      senseiReq = _reqRewriter.rewrite(senseiReq, node, partitions);
    }

    // customize only if user wants hits
    if (oldCount > 0)
    {
      senseiReq.setOffset(0);
      senseiReq.setCount(oldOffset + oldCount);
    }
    senseiReq.setPartitions(partitions);

    if (logger.isDebugEnabled())
    {
      logger.debug("scattering to partitions: " + partitions.toString());
    }
    return SenseiRequestBPOConverter.convert(senseiReq);
  }

}
