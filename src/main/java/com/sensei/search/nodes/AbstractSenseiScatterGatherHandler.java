package com.sensei.search.nodes;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.google.protobuf.Message;
import com.linkedin.norbert.javacompat.network.ScatterGatherHandler;
import com.sensei.search.req.AbstractSenseiRequest;
import com.sensei.search.req.AbstractSenseiResult;

public abstract class AbstractSenseiScatterGatherHandler<REQUEST extends AbstractSenseiRequest, RESULT extends AbstractSenseiResult, REQMSG extends Message, RESMSG extends Message>
    implements ScatterGatherHandler<RESULT, Integer>
{

  private final static Logger logger = Logger.getLogger(AbstractSenseiScatterGatherHandler.class);

  private final static long TIMEOUT_MILLIS = 8000L;

  private long _timeoutMillis = TIMEOUT_MILLIS;

  public void setTimeoutMillis(long timeoutMillis)
  {
    _timeoutMillis = timeoutMillis;
  }

  public long getTimeoutMillis()
  {
    return _timeoutMillis;
  }

  /**
   * Converts a message to a request object.
   * 
   * @param msg
   * @return
   */
  public abstract REQUEST messageToRequest(REQMSG msg);

  /**
   * Converts a result object to a message.
   * 
   * @param result
   * @return
   */
  public abstract RESMSG resultToMessage(RESULT result);

  public abstract REQMSG requestToMessage(REQUEST request);

  public abstract RESULT messageToResult(RESMSG message);

  /**
   * Merge results on the client/broker side. It likely works differently from
   * the one in the search node.
   * 
   * @param request
   *          the original request object
   * @param resultList
   *          the list of results from all the requested partitions.
   * @return one single result instance that is merged from the result list.
   */
  public abstract RESULT mergeResults(REQUEST request, List<RESULT> resultList);

  /*
   * (non-Javadoc)
   * 
   * @see
   * com.linkedin.norbert.network.javaapi.ScatterGatherHandler#gatherResponses
   * (com.google.protobuf.Message,
   * com.linkedin.norbert.network.ResponseIterator)
   */
  public RESULT gatherResponses(Message message, com.linkedin.norbert.network.ResponseIterator iter) throws Exception
  {
    boolean debugmode = logger.isDebugEnabled();
    @SuppressWarnings("unchecked")
    REQMSG req = (REQMSG) message;
    if (debugmode)
    {
      logger.debug("Converted the input Message to SenseiRequestBPO.Request");
    }
    REQUEST senseiReq = messageToRequest(req);
    if (debugmode)
    {
      logger.debug("Converted the SenseiRequestBPO.Request to SenseiRequest");
    }

    List<RESULT> boboBrowseList = new ArrayList<RESULT>();
    while (iter.hasNext())
    {
      @SuppressWarnings("unchecked")
      RESMSG boboMsg = (RESMSG) iter.next(_timeoutMillis > 0 ? _timeoutMillis : Long.MAX_VALUE, TimeUnit.MILLISECONDS);

      if (boboMsg == null)
      {
        logger.error("Request Timed Out");
      } else
      {
        RESULT res = messageToResult(boboMsg);
        if (debugmode)
        {
          logger.debug("Converting the SenseiResultBPO.Result from the iterator to SenseiResult");
          logger.debug("premerge results: " + res);
        }
        boboBrowseList.add(res);
      }
    }
    RESULT res = mergeResults(senseiReq, boboBrowseList);
    if (debugmode)
    {
      logger.debug("merged results: " + res);
      logger.debug("Merging the sensei Results for the input senseiRequest");
    }
    return res;
  }

}
