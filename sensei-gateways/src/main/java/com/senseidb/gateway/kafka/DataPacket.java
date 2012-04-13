package com.senseidb.gateway.kafka;

public final class DataPacket {

  public final byte[] data;
  public final int offset;
  public final int size;
  
  public DataPacket(byte[] data,int offset,int size){
    this.data = data;
    this.offset = offset;
    this.size = size;
  }
}
