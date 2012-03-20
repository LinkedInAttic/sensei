package com.sensei.search.req.protobuf;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import com.senseidb.search.req.SenseiRequest;
import com.senseidb.search.req.SenseiSystemInfo;
import org.apache.log4j.Logger;
import com.google.protobuf.ByteString;

public class SenseiSysRequestBPOConverter {

	private static Logger logger = Logger.getLogger(SenseiSysRequestBPOConverter.class);
	
	public static SenseiRequest convert(SenseiSysRequestBPO.SysRequest req)
	{
		try
		{
			ByteString value = req.getVal();
			byte[] raw = value.toByteArray();
			ByteArrayInputStream bais = new ByteArrayInputStream(raw);
			ObjectInputStream ois = new ObjectInputStream(bais);
			SenseiRequest ret = (SenseiRequest) ois.readObject();
			return ret;
		} catch (Exception e)
		{
			logger.error("serialize request", e);
		}
		return null;
	}
	public static SenseiSysRequestBPO.SysRequest convert(SenseiRequest req)
	{
		SenseiSysRequestBPO.SysRequest.Builder builder = SenseiSysRequestBPO.SysRequest.newBuilder();
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
		return SenseiSysRequestBPO.SysRequest.getDefaultInstance();
	}
	public static SenseiSystemInfo convert(SenseiSysResultBPO.SysResult res)
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
	public static SenseiSysResultBPO.SysResult convert(SenseiSystemInfo res)
	{
		SenseiSysResultBPO.SysResult.Builder builder = SenseiSysResultBPO.SysResult.newBuilder();
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
		return SenseiSysResultBPO.SysResult.getDefaultInstance();
	}
}

