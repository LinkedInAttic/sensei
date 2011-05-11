package com.sensei.test;

import com.linkedin.norbert.javacompat.network.RequestHandler;
import org.apache.log4j.Logger;

import com.google.protobuf.Message;
import com.sensei.search.req.SenseiGenericRequest;
import com.sensei.search.req.SenseiGenericResult;
import com.sensei.search.req.protobuf.SenseiGenericBPOConverter;
import com.sensei.search.req.protobuf.SenseiGenericRequestBPO;
import com.sensei.search.req.protobuf.SenseiGenericResultBPO;
import com.sensei.search.req.protobuf.SenseiGenericRequestBPO.GenericRequest;

public class GenericMessageHandler implements RequestHandler<SenseiGenericRequest, SenseiGenericResult>
{
  private final static Logger logger = Logger.getLogger(GenericMessageHandler.class);

  @Override
  public SenseiGenericResult handleRequest(SenseiGenericRequest request) throws Exception {
    SenseiGenericResult result = new SenseiGenericResult();
    result.setClassname("aaa");
    result.setResult("result aaa " + request.getClassname() + request.getRequest());
    return result;
//    logger.info("src: " + message.getDescriptorForType().getFullName() + "  " + res.getDescriptorForType().getFullName());
  }
}
