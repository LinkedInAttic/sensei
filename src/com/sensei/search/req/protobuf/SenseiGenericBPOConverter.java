package com.sensei.search.req.protobuf;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.apache.log4j.Logger;

import com.google.protobuf.ByteString;
import com.sensei.search.req.SenseiGenericRequest;
import com.sensei.search.req.SenseiGenericResult;

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
}
