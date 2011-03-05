package com.sensei.search.nodes;


import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import com.google.protobuf.Message;
import com.linkedin.norbert.javacompat.cluster.Node;
import com.sensei.search.client.ResultMerger;
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
    return ResultMerger.merge(request, resultList, false);
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
