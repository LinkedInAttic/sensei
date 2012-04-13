package com.sensei.search.req.protobuf;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import com.senseidb.search.req.SenseiGenericRequest;
import com.senseidb.search.req.SenseiGenericResult;
import org.apache.log4j.Logger;

import com.google.protobuf.ByteString;

public class SenseiGenericBPOConverter
{
  private static Logger logger = Logger.getLogger(SenseiGenericBPOConverter.class);
  public static SenseiGenericRequest convert(SenseiGenericRequestBPO.GenericRequest req)
  {
    try
    {
      String classname = req.getClassname();
      ByteString value = req.getVal();
      byte[] raw = value.toByteArray();
      ByteArrayInputStream bais = new ByteArrayInputStream(raw);
      ObjectInputStream ois = new ObjectInputStream(bais);
      SenseiGenericRequest ret = new SenseiGenericRequest();
      ret.setClassname(classname);
      ret.setRequest((Serializable) ois.readObject());
      return ret;
    } catch (Exception e)
    {
      logger.error("serialize request", e);
    }
    return (SenseiGenericRequest) null;
  }
  public static SenseiGenericRequestBPO.GenericRequest convert(SenseiGenericRequest req)
  {
    SenseiGenericRequestBPO.GenericRequest.Builder builder = SenseiGenericRequestBPO.GenericRequest.newBuilder();
    try
    {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ObjectOutputStream oos;
      oos = new ObjectOutputStream(baos);
      oos.writeObject(req.getRequest());
      oos.close();
      byte[] raw = baos.toByteArray();
      builder.setClassname(req.getClassname());
      builder.setVal(ByteString.copyFrom(raw));
      return builder.build();
    } catch (IOException e)
    {
      logger.error("deserialize request", e);
    }
    return SenseiGenericRequestBPO.GenericRequest.getDefaultInstance();
  }
  public static SenseiGenericResult convert(SenseiGenericResultBPO.GenericResult req)
  {
    try
    {
      String classname = req.getClassname();
      ByteString value = req.getVal();
      byte[] raw = value.toByteArray();
      ByteArrayInputStream bais = new ByteArrayInputStream(raw);
      ObjectInputStream ois = new ObjectInputStream(bais);
      SenseiGenericResult ret = new SenseiGenericResult();
      ret.setClassname(classname);
      ret.setResult((Serializable) ois.readObject());
      return ret;
    } catch (Exception e)
    {
      logger.error("serialize result", e);
    }
    return (SenseiGenericResult) null;
  }
  public static SenseiGenericResultBPO.GenericResult convert(SenseiGenericResult req)
  {
    SenseiGenericResultBPO.GenericResult.Builder builder = SenseiGenericResultBPO.GenericResult.newBuilder();
    try
    {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ObjectOutputStream oos;
      oos = new ObjectOutputStream(baos);
      oos.writeObject(req.getResult());
      oos.close();
      byte[] raw = baos.toByteArray();
      builder.setClassname(req.getClassname());
      builder.setVal(ByteString.copyFrom(raw));
      return builder.build();
    } catch (IOException e)
    {
      logger.error("deserialize result", e);
    }
    return SenseiGenericResultBPO.GenericResult.getDefaultInstance();
  }

  public static byte[] decompress(byte[] output) throws IOException
  {
    ByteArrayInputStream bais = new ByteArrayInputStream(output);
    GZIPInputStream gzis = new GZIPInputStream(bais);
    byte[] buf = new byte[2048];
    List<byte[]> list = new LinkedList<byte[]>();
    int len = gzis.read(buf, 0, 2048);
    int i = 0;
    while(len>0)
    {
      byte[] b1 = new byte[len];
      System.arraycopy(buf, 0, b1, 0, len);
      list.add(b1);
      i+= len;
      len = gzis.read(buf, 0, 2048);
    }
    gzis.close();
    byte[] whole = new byte[i];
    int start = 0;
    for(byte[] part : list)
    {
      System.arraycopy(part, 0, whole, start, part.length);
      start += part.length;
    }
    return whole;
  }

  public static byte[] compress(byte[] b) throws IOException
  {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    GZIPOutputStream gzos = new GZIPOutputStream(baos);
    gzos.write(b);
    gzos.close();
    byte[] output = baos.toByteArray();
    return output;
  }
}
