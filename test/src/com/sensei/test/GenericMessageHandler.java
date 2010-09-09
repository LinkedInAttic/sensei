package com.sensei.test;

import org.apache.log4j.Logger;

import com.google.protobuf.Message;
import com.linkedin.norbert.network.javaapi.MessageHandler;
import com.sensei.search.req.SenseiGenericRequest;
import com.sensei.search.req.SenseiGenericResult;
import com.sensei.search.req.protobuf.SenseiGenericBPOConverter;
import com.sensei.search.req.protobuf.SenseiGenericRequestBPO;
import com.sensei.search.req.protobuf.SenseiGenericResultBPO;
import com.sensei.search.req.protobuf.SenseiGenericRequestBPO.GenericRequest;

public class GenericMessageHandler implements MessageHandler
{
  private final static Logger logger = Logger.getLogger(GenericMessageHandler.class);
  @Override
  public Message handleMessage(Message message) throws Exception
  {
    SenseiGenericRequestBPO.GenericRequest req = (GenericRequest) message;
    SenseiGenericRequest request = SenseiGenericBPOConverter.convert(req);
    SenseiGenericResult result = new SenseiGenericResult();
    result.setClassname("aaa");
    result.setResult("result aaa " + request.getClassname() + request.getRequest());
    SenseiGenericResultBPO.GenericResult res = SenseiGenericBPOConverter.convert(result);
    logger.info("src: " + message.getDescriptorForType().getFullName() + "  " + res.getDescriptorForType().getFullName());
    return res;
  }

}
