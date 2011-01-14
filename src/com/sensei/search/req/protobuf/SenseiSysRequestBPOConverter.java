package com.sensei.search.req.protobuf;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.log4j.Logger;
import com.google.protobuf.ByteString;
import com.sensei.search.req.SenseiSystemInfo;

public class SenseiSysRequestBPOConverter {

	private static Logger logger = Logger.getLogger(SenseiSysRequestBPOConverter.class);
	
	public static Object convert(SenseiSysRequestBPO.Request req)
	{
		try
		{
			ByteString value = req.getVal();
			byte[] raw = value.toByteArray();
			ByteArrayInputStream bais = new ByteArrayInputStream(raw);
			ObjectInputStream ois = new ObjectInputStream(bais);
			Object ret = (Object) ois.readObject();
			return ret;
		} catch (Exception e)
		{
			logger.error("serialize request", e);
		}
		return null;
	}
	public static SenseiSysRequestBPO.Request convert(Object req)
	{
		SenseiSysRequestBPO.Request.Builder builder = SenseiSysRequestBPO.Request.newBuilder();
		try
		{
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutputStream oos;
			oos = new ObjectOutputStream(baos);
			oos.writeObject(req);
			oos.close();
			byte[] raw = baos.toByteArray();
			builder.setVal(ByteString.copyFrom(raw));
			return builder.build();
		} catch (IOException e)
		{
			logger.error("deserialize request", e);
		}
		return SenseiSysRequestBPO.Request.getDefaultInstance();
	}
	public static SenseiSystemInfo convert(SenseiSysResultBPO.Result res)
	{
		try
		{
			ByteString value = res.getVal();
			byte[] raw = value.toByteArray();
			ByteArrayInputStream bais = new ByteArrayInputStream(raw);
			ObjectInputStream ois = new ObjectInputStream(bais);
			SenseiSystemInfo ret = (SenseiSystemInfo) ois.readObject();
			return ret;
		} catch (Exception e)
		{
			logger.error("serialize result", e);
		}
		return null;
	}
	public static SenseiSysResultBPO.Result convert(SenseiSystemInfo res)
	{
		SenseiSysResultBPO.Result.Builder builder = SenseiSysResultBPO.Result.newBuilder();
		try
		{
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutputStream oos;
			oos = new ObjectOutputStream(baos);
			oos.writeObject(res);
			oos.close();
			byte[] raw = baos.toByteArray();
			builder.setVal(ByteString.copyFrom(raw));
			return builder.build();
		} catch (IOException e)
		{
			logger.error("deserialize result", e);
		}
		return SenseiSysResultBPO.Result.getDefaultInstance();
	}
}

