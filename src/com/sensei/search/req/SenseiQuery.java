package com.sensei.search.req;

public final class SenseiQuery {
  private byte[] _bytes;
  public SenseiQuery(byte[] bytes){
	  _bytes = bytes;
  }
  
  public byte[] toBytes(){
	  return _bytes;
  }
}
