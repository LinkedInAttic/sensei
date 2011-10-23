package com.sensei.test;

import org.apache.log4j.Logger;

import com.linkedin.norbert.javacompat.network.RequestHandler;
import com.sensei.search.req.SenseiGenericRequest;
import com.sensei.search.req.SenseiGenericResult;

public class GenericMessageHandler implements RequestHandler<SenseiGenericRequest, SenseiGenericResult>
{
  private final static Logger logger = Logger.getLogger(GenericMessageHandler.class);

  @Override
  public SenseiGenericResult handleRequest(SenseiGenericRequest request) throws Exception {
      SenseiGenericResult result = new SenseiGenericResult();
      result.setClassname("aaa");
      result.setResult("result aaa " + request.getClassname() + request.getRequest());
      return result;
  }
}
