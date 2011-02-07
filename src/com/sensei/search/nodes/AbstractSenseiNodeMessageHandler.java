package com.sensei.search.nodes;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import proj.zoie.api.IndexReaderFactory;
import proj.zoie.api.ZoieIndexReader;
import com.browseengine.bobo.api.BoboIndexReader;
import com.google.protobuf.Message;
import com.google.protobuf.TextFormat;
import com.linkedin.norbert.javacompat.network.MessageHandler;
import com.sensei.search.req.AbstractSenseiRequest;
import com.sensei.search.req.AbstractSenseiResult;
import com.sensei.search.req.protobuf.SenseiRequestBPO;

/**
 * @author "Xiaoyang Gu<xgu@linkedin.com>"
 *
 * @param <REQUEST>
 * @param <RESULT>
 */
public abstract class AbstractSenseiNodeMessageHandler<REQUEST extends AbstractSenseiRequest, RESULT extends AbstractSenseiResult> implements MessageHandler
{

  private static final Logger logger = Logger.getLogger(AbstractSenseiNodeMessageHandler.class);
  protected final Map<Integer, SenseiQueryBuilderFactory> _builderFactoryMap;
  protected final Map<Integer, IndexReaderFactory<ZoieIndexReader<BoboIndexReader>>> _partReaderMap;

  public AbstractSenseiNodeMessageHandler(SenseiSearchContext ctx)
  {
    _builderFactoryMap = ctx.getQueryBuilderFactoryMap();
    _partReaderMap = ctx.getPartitionReaderMap();
  }

  public int[] getPartitions()
  {
    Set<Integer> partSet = _partReaderMap.keySet();
    int[] retSet = new int[partSet.size()];
    int c = 0;
    for (Integer part : partSet)
    {
      retSet[c++] = part;
    }
    return retSet;
  }

  /**
   * @return default request message instance. This helps norbert to decide what kind of request
   * this handler can handle and route the message properly. A server may be able to handle multiple
   * types of messages.
   */
  public abstract Message[] getMessages();

  /**
   * The actual method that process the request for one partition given a list of index readers.
   * @param request
   * @param partition the partition id
   * @param readerList
   * @return the result
   * @throws Exception
   */
  public abstract RESULT handleMessage(final REQUEST request, int partition, final List<ZoieIndexReader<BoboIndexReader>> readerList) throws Exception;

  /**
   * Converts a message to a request object.
   * @param msg
   * @return
   */
  public abstract REQUEST messageToRequest(Message msg);

  /**
   * Converts a result object to a message.
   * @param result
   * @return
   */
  public abstract Message resultToMessage(RESULT result);

  /**
   * Merge results on the server side.
   * @param request the original request object
   * @param resultList the list of results from all the requested partitions.
   * @return one single result instance that is merged from the result list.
   */
  public abstract RESULT mergeResults(REQUEST request, List<RESULT> resultList);

  /**
   * @return an empty result instance. Used when the request cannot be properly processed or
   * when the true result is empty.
   */
  public abstract RESULT getEmptyResultInstance();

  private RESULT handleMessage(REQUEST request, IndexReaderFactory<ZoieIndexReader<BoboIndexReader>> readerFactory, int partition) throws Exception
  {
    List<ZoieIndexReader<BoboIndexReader>> readerList = null;
    try
    {
      readerList = readerFactory.getIndexReaders();
      return handleMessage(request, partition, readerList);
    } finally
    {
      if (readerList != null)
      {
        readerFactory.returnIndexReaders(readerList);
      }
    }
  }

  /**
   * The default implementation of the message handler. It controls the entire overall flow of the message
   * processing. It iterates all the requested partitions and process the message on each partition and then
   * merge the results from all the partitions into one result and convert it to a result message and send
   * it back. 
   * @see com.linkedin.norbert.javacompat.network.MessageHandler#handleMessage(com.google.protobuf.Message)
   */
  public Message handleMessage(Message msg) throws Exception
  {
    SenseiRequestBPO.Request req = (SenseiRequestBPO.Request) msg;

    if (logger.isDebugEnabled())
    {
      String reqString = TextFormat.printToString(req);
      reqString = reqString.replace('\r', ' ').replace('\n', ' ');
    }

    REQUEST senseiReq = messageToRequest(req);
    RESULT finalResult = null;
    Set<Integer> partitions = senseiReq.getPartitions();
    if (partitions != null && partitions.size() > 0)
    {
      logger.info("serving partitions: " + partitions.toString());
      ArrayList<RESULT> resultList = new ArrayList<RESULT>(partitions.size());
      for (int partition : partitions)
      {
        try
        {
          long start = System.currentTimeMillis();
          IndexReaderFactory<ZoieIndexReader<BoboIndexReader>> readerFactory = _partReaderMap.get(partition);
          RESULT res = handleMessage(senseiReq, readerFactory, partition);
          resultList.add(res);
          long end = System.currentTimeMillis();
          res.setTime(end - start);
          logger.info("searching partition: " + partition + " browse took: " + res.getTime());
        } catch (Exception e)
        {
          logger.error(e.getMessage(), e);
        }
      }

      finalResult = mergeResults(senseiReq, resultList);
    } else
    {
      logger.info("no partitions specified");
      finalResult = getEmptyResultInstance();
    }
    Message returnvalue = resultToMessage(finalResult);
    logger.info("searching partitions  " + partitions.toString() + " took: " + finalResult.getTime());
    return returnvalue;
  }
}
