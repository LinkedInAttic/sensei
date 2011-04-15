package com.sensei.search.svc.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.lucene.search.Query;

import proj.zoie.api.ZoieVersion;
import proj.zoie.api.ZoieIndexReader;
import proj.zoie.api.ZoieIndexReader.SubReaderAccessor;
import proj.zoie.api.ZoieIndexReader.SubReaderInfo;
import proj.zoie.mbean.ZoieSystemAdminMBean;
import proj.zoie.impl.indexing.ZoieSystem;

import com.browseengine.bobo.api.BoboBrowser;
import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.api.BrowseException;
import com.browseengine.bobo.api.BrowseHit;
import com.browseengine.bobo.api.BrowseRequest;
import com.browseengine.bobo.api.BrowseResult;
import com.browseengine.bobo.api.FacetAccessible;
import com.browseengine.bobo.api.MultiBoboBrowser;
import com.google.protobuf.Message;
import com.sensei.search.client.ResultMerger;
import com.sensei.search.nodes.SenseiCore;
import com.sensei.search.nodes.SenseiQueryBuilderFactory;
import com.sensei.search.req.SenseiRequest;
import com.sensei.search.req.SenseiSystemInfo;
import com.sensei.search.req.protobuf.SenseiSysRequestBPO;
import com.sensei.search.req.protobuf.SenseiSysRequestBPOConverter;
import com.sensei.search.util.RequestConverter;

public class SysSenseiCoreServiceImpl extends AbstractSenseiCoreService<SenseiRequest, SenseiSystemInfo>{

  private static final Logger logger = Logger.getLogger(SysSenseiCoreServiceImpl.class);
  
  public SysSenseiCoreServiceImpl(SenseiCore core) {
    super(core);
  }
  
  @Override
  public SenseiSystemInfo handlePartitionedRequest(SenseiRequest request,
      List<BoboIndexReader> readerList,SenseiQueryBuilderFactory queryBuilderFactory) throws Exception {
    SenseiSystemInfo res = new SenseiSystemInfo();

    MultiBoboBrowser browser = null;
    try
    {
      browser = new MultiBoboBrowser(BoboBrowser.createBrowsables(readerList));
      res.setNumDocs(browser.numDocs());

      return res;
    }
    catch (Exception e)
    {
      logger.error(e.getMessage(), e);
      throw e;
    }
    finally
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
  public SenseiSystemInfo mergePartitionedResults(SenseiRequest r,
      List<SenseiSystemInfo> resultList) {
    SenseiSystemInfo result = _core.getSystemInfo();
    result.setNumDocs(0);
    for (SenseiSystemInfo res : resultList)
    {
      result.setNumDocs(result.getNumDocs() + res.getNumDocs());
    }

    return result;
  }

  @Override
  public SenseiSystemInfo getEmptyResultInstance(Throwable error) {
    return new SenseiSystemInfo();
  } 
    
  @Override
  public Message resultToMessage(SenseiSystemInfo result) {
    return SenseiSysRequestBPOConverter.convert(result);
  }

  @Override
  public SenseiRequest reqFromMessage(Message req) {
    return SenseiSysRequestBPOConverter.convert((SenseiSysRequestBPO.SysRequest)req);
  }

  @Override
  public Message getEmptyRequestInstance() {
    return SenseiSysRequestBPO.SysRequest.getDefaultInstance(); 
  }
}

