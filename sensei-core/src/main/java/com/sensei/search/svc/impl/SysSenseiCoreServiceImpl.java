package com.sensei.search.svc.impl;

import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;

import com.browseengine.bobo.api.BoboBrowser;
import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.api.MultiBoboBrowser;
import com.linkedin.norbert.network.JavaSerializer;
import com.linkedin.norbert.network.Serializer;
import com.sensei.search.nodes.SenseiCore;
import com.sensei.search.nodes.SenseiQueryBuilderFactory;
import com.sensei.search.req.protobuf.SenseiSysReqProtoSerializer;
import com.sensei.search.req.SenseiRequest;
import com.sensei.search.req.SenseiSystemInfo;

public class SysSenseiCoreServiceImpl extends AbstractSenseiCoreService<SenseiRequest, SenseiSystemInfo>{
	public static final Serializer<SenseiRequest, SenseiSystemInfo> SERIALIZER =
			JavaSerializer.apply("SenseiSysRequest", SenseiRequest.class, SenseiSystemInfo.class);

	public static final Serializer<SenseiRequest, SenseiSystemInfo> PROTO_SERIALIZER =
			new SenseiSysReqProtoSerializer();

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
	public Serializer<SenseiRequest, SenseiSystemInfo> getSerializer() {
		return SERIALIZER;
	}
}

