package com.sensei.search.nodes;

import java.io.IOException;
import java.util.List;
import org.apache.log4j.Logger;
import org.apache.lucene.search.Query;

import proj.zoie.api.ZoieIndexReader;
import proj.zoie.api.ZoieIndexReader.SubReaderAccessor;
import proj.zoie.api.ZoieIndexReader.SubReaderInfo;

import com.browseengine.bobo.api.BoboBrowser;
import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.api.BrowseException;
import com.browseengine.bobo.api.BrowseHit;
import com.browseengine.bobo.api.BrowseRequest;
import com.browseengine.bobo.api.BrowseResult;
import com.browseengine.bobo.api.MultiBoboBrowser;
import com.google.protobuf.Message;
import com.sensei.search.client.ResultMerger;
import com.sensei.search.req.SenseiHit;
import com.sensei.search.req.SenseiRequest;
import com.sensei.search.req.SenseiResult;
import com.sensei.search.req.protobuf.SenseiRequestBPO;
import com.sensei.search.req.protobuf.SenseiRequestBPOConverter;
import com.sensei.search.util.RequestConverter;

public class SenseiNodeMessageHandler extends AbstractSenseiNodeMessageHandler<SenseiRequest, SenseiResult>
{

  private static final Logger logger = Logger.getLogger(SenseiNodeMessageHandler.class);

  public SenseiNodeMessageHandler(SenseiSearchContext ctx)
  {
    super(ctx);
  }

  @Override
  public Message[] getMessages()
  {
    return new Message[] { SenseiRequestBPO.Request.getDefaultInstance() };
  }

  private SenseiResult browse(MultiBoboBrowser browser, BrowseRequest req, SubReaderAccessor<BoboIndexReader> subReaderAccessor) throws BrowseException
  {
    final SenseiResult result = new SenseiResult();

    long start = System.currentTimeMillis();
    int offset = req.getOffset();
    int count = req.getCount();

    if (offset < 0 || count < 0)
    {
      throw new IllegalArgumentException("both offset and count must be > 0: " + offset + "/" + count);
    }
    // SortCollector collector =
    // browser.getSortCollector(req.getSort(),req.getQuery(), offset, count,
    // req.isFetchStoredFields(),false);

    // Map<String, FacetAccessible> facetCollectors = new HashMap<String,
    // FacetAccessible>();
    // browser.browse(req, collector, facetCollectors);
    BrowseResult res = browser.browse(req);
    BrowseHit[] hits = res.getHits();

    SenseiHit[] senseiHits = new SenseiHit[hits.length];
    for (int i = 0; i < hits.length; i++)
    {
      BrowseHit hit = hits[i];
      SenseiHit senseiHit = new SenseiHit();

      int docid = hit.getDocid();
      SubReaderInfo<BoboIndexReader> readerInfo = subReaderAccessor.getSubReaderInfo(docid);
      long uid = (long) ((ZoieIndexReader<BoboIndexReader>) readerInfo.subreader.getInnerReader()).getUID(readerInfo.subdocid);
      senseiHit.setUID(uid);
      senseiHit.setDocid(docid);
      senseiHit.setScore(hit.getScore());
      senseiHit.setComparable(hit.getComparable());
      senseiHit.setFieldValues(hit.getFieldValues());
      senseiHit.setStoredFields(hit.getStoredFields());
      senseiHit.setExplanation(hit.getExplanation());

      senseiHits[i] = senseiHit;
    }
    result.setHits(senseiHits);
    result.setNumHits(res.getNumHits());
    result.setTotalDocs(browser.numDocs());
    result.addAll(res.getFacetMap());
    long end = System.currentTimeMillis();
    result.setTime(end - start);
    // set the transaction ID to trace transactions
    result.setTid(req.getTid());

    Query parsedQ = req.getQuery();
    if (parsedQ != null)
    {
      result.setParsedQuery(parsedQ.toString());
    } else
    {
      result.setParsedQuery("*:*");
    }
    return result;
  }

  @Override
  public SenseiResult getEmptyResultInstance()
  {
    return new SenseiResult();
  }

  @Override
  public SenseiResult handleMessage(SenseiRequest request, int partition, List<ZoieIndexReader<BoboIndexReader>> readerList) throws Exception
  {
    if (readerList == null || readerList.size() == 0)
    {
      logger.warn("no readers were obtained from zoie, returning no hits.");
      return new SenseiResult();
    }
    List<BoboIndexReader> boboReaders = ZoieIndexReader.extractDecoratedReaders(readerList);
    SubReaderAccessor<BoboIndexReader> subReaderAccessor = ZoieIndexReader.getSubReaderAccessor(boboReaders);
    MultiBoboBrowser browser = null;
    try
    {
      browser = new MultiBoboBrowser(BoboBrowser.createBrowsables(boboReaders));
      BrowseRequest breq = RequestConverter.convert(request, _builderFactoryMap.get(partition));
      SenseiResult res = browse(browser, breq, subReaderAccessor);
      return res;
    } catch (Exception e)
    {
      logger.error(e.getMessage(), e);
      throw e;
    } finally
    {
      if (browser != null)
      {
        try
        {
          browser.close();
        } catch (IOException ioe)
        {
          logger.error(ioe.getMessage(), ioe);
        }
      }
    }
  }

  @Override
  public SenseiResult mergeResults(SenseiRequest request, List<SenseiResult> resultList)
  {
    return ResultMerger.merge(request, resultList, true);
  }

  @Override
  public SenseiRequest messageToRequest(Message msg)
  {
    return SenseiRequestBPOConverter.convert((SenseiRequestBPO.Request)msg);
  }

  @Override
  public Message resultToMessage(SenseiResult result)
  {
    return SenseiRequestBPOConverter.convert(result);
  }

}
