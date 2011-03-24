package com.sensei.search.nodes;

import org.apache.log4j.Logger;

import com.google.protobuf.Message;
import com.sensei.search.req.SenseiRequest;
import com.sensei.search.req.SenseiResult;
import com.sensei.search.req.protobuf.SenseiRequestBPO;
import com.sensei.search.req.protobuf.SenseiResultBPO;
import com.sensei.search.svc.impl.CoreSenseiServiceImpl;

public class SenseiNodeMessageHandler extends AbstractSenseiNodeMessageHandler
{

  private static final Logger logger = Logger.getLogger(SenseiNodeMessageHandler.class);
  private final CoreSenseiServiceImpl _coreSvc;
  
  public SenseiNodeMessageHandler(CoreSenseiServiceImpl coreSvc){
	  _coreSvc = coreSvc;
  }

  @Override
  public Message getRequestMessage()
  {
    return SenseiRequestBPO.Request.getDefaultInstance();
  }
  
  @Override
  public Message getResponseMessage()
  {
    return SenseiResultBPO.Result.getDefaultInstance();
  }

  
  @Override
  public Message handleMessage(Message req) throws Exception {
	  
	SenseiRequest sreq = _coreSvc.reqFromMessage(req);
	SenseiResult sres = _coreSvc.execute(sreq);
	return _coreSvc.resultToMessage(sres);
  }

}
