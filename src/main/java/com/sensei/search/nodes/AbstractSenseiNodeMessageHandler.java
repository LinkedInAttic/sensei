package com.sensei.search.nodes;

import org.apache.log4j.Logger;

import com.google.protobuf.Message;
import com.linkedin.norbert.javacompat.network.MessageHandler;

/**
 * @author "Xiaoyang Gu<xgu@linkedin.com>"
 */
public abstract class AbstractSenseiNodeMessageHandler implements MessageHandler
{

  private static final Logger logger = Logger.getLogger(AbstractSenseiNodeMessageHandler.class);

  public AbstractSenseiNodeMessageHandler(){
  }

  
  /**
   * @return default request message instance. This helps norbert to decide what kind of request
   * this handler can handle and route the message properly. A server may be able to handle multiple
   * types of messages.
   */
  public abstract Message getRequestMessage();
  

  public abstract Message getResponseMessage();

}
