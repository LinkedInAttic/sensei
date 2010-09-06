package com.sensei.search.nodes;


import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.google.protobuf.Message;
import com.google.protobuf.TextFormat.ParseException;
import com.linkedin.norbert.javacompat.cluster.Node;
import com.linkedin.norbert.javacompat.network.ScatterGatherHandler;
import com.sensei.search.client.ResultMerger;
import com.sensei.search.req.SenseiRequest;
import com.sensei.search.req.SenseiResult;
import com.sensei.search.req.protobuf.SenseiRequestBPO;
import com.sensei.search.req.protobuf.SenseiRequestBPOConverter;
import com.sensei.search.req.protobuf.SenseiResultBPO;

public class SenseiScatterGatherHandler implements ScatterGatherHandler<SenseiResult, Integer> 
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

  public Message customizeMessage(Message msg, Node node, Set<Integer> partitions) throws Exception
  {
    SenseiRequestBPO.Request req = (SenseiRequestBPO.Request)msg;
    try{
      SenseiRequest senseiReq = SenseiRequestBPOConverter.convert(req);
      
      int oldOffset = senseiReq.getOffset();
      int oldCount = senseiReq.getCount();
      if (_reqRewriter!=null){
          senseiReq = _reqRewriter.rewrite(senseiReq, node, partitions);
      }
      
      senseiReq.setOffset(0);
      senseiReq.setCount(oldOffset+oldCount);
      senseiReq.setPartitions(partitions);

      if (logger.isDebugEnabled())
      {
        logger.debug("scattering to partitions: "+ partitions.toString());
      }
      return SenseiRequestBPOConverter.convert(senseiReq);
    }
    catch(ParseException pe){
      throw new RuntimeException(pe.getMessage(),pe);
    } 
  }
  
  /* (non-Javadoc)
   * @see com.linkedin.norbert.network.javaapi.ScatterGatherHandler#gatherResponses(com.google.protobuf.Message, com.linkedin.norbert.network.ResponseIterator)
   */
  public SenseiResult gatherResponses(Message message,
                                      com.linkedin.norbert.network.ResponseIterator iter) throws Exception
  {
    SenseiRequestBPO.Request req = (SenseiRequestBPO.Request)message;
    if (logger.isDebugEnabled())
    {
      logger.debug("Converted the input Message to SenseiRequestBPO.Request");
    }
    try{
      SenseiRequest senseiReq = SenseiRequestBPOConverter.convert(req);
      if (logger.isDebugEnabled())
      {
        logger.debug("Converted the SenseiRequestBPO.Request to SenseiRequest");
      }
      int oldOffset = senseiReq.getOffset();
      int oldCount = senseiReq.getCount();
      
      List<SenseiResult> boboBrowseList = new ArrayList<SenseiResult>();
      while(iter.hasNext()){
        Message boboMsg = iter.next(_timeoutMillis>0 ? _timeoutMillis : Long.MAX_VALUE,TimeUnit.MILLISECONDS);

        if (boboMsg==null){
          logger.error("Request Timed Out");
        }
        else {
            SenseiResult res = SenseiRequestBPOConverter.convert((SenseiResultBPO.Result)boboMsg);
            if (logger.isDebugEnabled())
            {
              logger.debug("Converting the SenseiResultBPO.Result from the iterator to SenseiResult");
              logger.debug("premerge results: " + res);
            }
            boboBrowseList.add(res);
        }
      }
          
      senseiReq.setOffset(oldOffset);
      senseiReq.setCount(oldCount);
      SenseiResult res = ResultMerger.merge(senseiReq, boboBrowseList, false);
      if (logger.isDebugEnabled())
      {
        logger.debug("merged results: " + res);
        logger.debug("Merging the sensei Results for the input senseiRequest");
      }
      return res;
    }
    catch(ParseException pe){
      throw new RuntimeException(pe.getMessage(),pe);
    }
  }

}
