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
import com.sensei.search.req.SenseiSystemInfo;
import com.sensei.search.req.protobuf.SenseiSysRequestBPO;
import com.sensei.search.req.protobuf.SenseiSysRequestBPOConverter;
import com.sensei.search.req.protobuf.SenseiSysResultBPO;

public class SenseiSysScatterGatherHandler implements ScatterGatherHandler<SenseiSystemInfo, Integer> 
{

  private final static Logger logger = Logger.getLogger(SenseiSysScatterGatherHandler.class);
  
  private final static long TIMEOUT_MILLIS = 8000L;

  private long _timeoutMillis = TIMEOUT_MILLIS;

  public void setTimeoutMillis(long timeoutMillis){
	  _timeoutMillis = timeoutMillis;
  }
  
  public long getTimeoutMillis(){
	  return _timeoutMillis;
  }

  public Message customizeMessage(Message msg, Node node, Set<Integer> partitions) throws Exception
  {
  	return msg;
  }
  
  public SenseiSystemInfo gatherResponses(Message message, com.linkedin.norbert.network.ResponseIterator iter) throws Exception
  {
    SenseiSystemInfo result = new SenseiSystemInfo();
    while (iter.hasNext())
    {
      Message boboMsg = iter.next(_timeoutMillis > 0 ? _timeoutMillis : Long.MAX_VALUE, TimeUnit.MILLISECONDS);

      if (boboMsg == null)
      {
        logger.error("Request Timed Out");
      } else
      {
        SenseiSystemInfo res = SenseiSysRequestBPOConverter.convert((SenseiSysResultBPO.SysResult) boboMsg);
        result.setNumDocs(result.getNumDocs() + res.getNumDocs());
        if (result.getLastModified() < res.getLastModified())
          result.setLastModified(res.getLastModified());
        if (Long.valueOf(result.getVersion()) < Long.valueOf(res.getVersion()))
          result.setVersion(res.getVersion());
      }
    }

    return result;
  }

}

