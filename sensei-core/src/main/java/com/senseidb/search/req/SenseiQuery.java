package com.senseidb.search.req;

import java.io.Serializable;
import java.nio.charset.Charset;

public class SenseiQuery implements Serializable
{
  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  private byte[] _bytes;
  public static Charset UTF_8_CHARSET = Charset.forName("UTF-8");
	
  public SenseiQuery(byte[] bytes){
	  _bytes = bytes;
  }
  
  final public byte[] toBytes(){
	  return _bytes;
  }
  
  @Override
  public String toString()
  {
	return new String(_bytes, UTF_8_CHARSET);
  }
}
