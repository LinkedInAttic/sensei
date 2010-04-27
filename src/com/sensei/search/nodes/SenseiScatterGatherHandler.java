package com.sensei.search.nodes;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.google.protobuf.Message;
import com.google.protobuf.TextFormat.ParseException;
import com.linkedin.norbert.cluster.Node;
import com.linkedin.norbert.network.javaapi.ScatterGatherHandler;
import com.sensei.search.client.ResultMerger;
import com.sensei.search.req.SenseiRequest;
import com.sensei.search.req.SenseiResult;
import com.sensei.search.req.protobuf.SenseiRequestBPO;
import com.sensei.search.req.protobuf.SenseiRequestBPOConverter;
import com.sensei.search.req.protobuf.SenseiResultBPO;

public class SenseiScatterGatherHandler implements ScatterGatherHandler<SenseiResult, Integer> 
{

  private final static Logger logger = Logger.getLogger(SenseiScatterGatherHandler.class);
  
  private final static long TIMEOUT_MILLIS = 8000;

  private final SenseiRequestScatterRewriter _reqRewriter;

  public SenseiScatterGatherHandler(SenseiRequestScatterRewriter reqRewriter)
  {
    _reqRewriter = reqRewriter;
  }

  public Message customizeMessage(Message msg, Node node, List<Integer> partitionList) throws Exception
  {
    SenseiRequestBPO.Request req = (SenseiRequestBPO.Request)msg;
    try{
      SenseiRequest senseiReq = SenseiRequestBPOConverter.convert(req);
      logger.info("Converted Message to SenseiReqeust method");
      
      int oldOffset = senseiReq.getOffset();
      int oldCount = senseiReq.getCount();
      Integer[] partitions = new Integer[partitionList.size()];
      for(int i = 0;i < partitionList.size();i ++)
      {
        partitions[i] = partitionList.get(i);
      }
      if (_reqRewriter!=null){
          senseiReq = _reqRewriter.rewrite(senseiReq, node, partitions);
      }
      
      senseiReq.setOffset(0);
      senseiReq.setCount(oldOffset+oldCount);
      senseiReq.setPartitions(partitions);

      logger.info("scattering to partitions: "+ Arrays.toString(partitions));
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
    logger.info("Converted the input Message to SenseiRequestBPO.Request");
    try{
      SenseiRequest senseiReq = SenseiRequestBPOConverter.convert(req);
      logger.info("Converted the SenseiRequestBPO.Request to SenseiRequest");
      int oldOffset = senseiReq.getOffset();
      int oldCount = senseiReq.getCount();
      
      List<SenseiResult> boboBrowseList = new ArrayList<SenseiResult>();
      while(iter.hasNext()){
        logger.info("Trying to fetch the next Message from iterator");
        Message boboMsg = iter.next(TIMEOUT_MILLIS,TimeUnit.MILLISECONDS);
        logger.info("Fetched the next message");

        if (boboMsg==null){
          logger.error("Request Timed Out");
        }
        else {
            SenseiResult res = SenseiRequestBPOConverter.convert((SenseiResultBPO.Result)boboMsg);
            logger.info("Converting the SenseiResultBPO.Result from the iterator to SenseiResult");
            boboBrowseList.add(res);
        }
      }
          
      senseiReq.setOffset(oldOffset);
      senseiReq.setCount(oldCount);
      SenseiResult res = ResultMerger.merge(senseiReq, boboBrowseList);
      logger.info("Mergin the sensei Results for the input senseiRequest");
      return res;
    }
    catch(ParseException pe){
      throw new RuntimeException(pe.getMessage(),pe);
    }
  }
}
