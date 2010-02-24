package com.sensei.search.req.protobuf;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.log4j.Logger;

import com.google.protobuf.ByteString;
import com.google.protobuf.TextFormat.ParseException;

public class ProtoConvertUtil {

	private final static Logger logger = Logger.getLogger(ProtoConvertUtil.class);
	public static Object serializeIn(ByteString byteString) throws ParseException{
		if (byteString==null) return null;
		try{
		  byte[] bytes = byteString.toByteArray();
		  if (bytes.length==0) return null;
		  ByteArrayInputStream bin = new ByteArrayInputStream(bytes);
		  ObjectInputStream oin = new ObjectInputStream(bin);
		  return oin.readObject();
		}
		catch(Exception e){
		  logger.error(e.getMessage(),e);
		  throw new ParseException(e.getMessage());
		}
	}
	
	public static ByteString serializeOut(Object o) throws ParseException{
		if (o == null) return null;
		try{
		  ByteArrayOutputStream bout = new ByteArrayOutputStream();
		  ObjectOutputStream oout = new ObjectOutputStream(bout);
		  oout.writeObject(o);
		  oout.flush();
		  byte[] data = bout.toByteArray();
		  return ByteString.copyFrom(data);
		}
		catch(Exception e){
			logger.error(e.getMessage(),e);
			throw new ParseException(e.getMessage());
		}
	}
	
	public static int[] toIntArray(ByteString byteString) throws ParseException{
		return (int[])serializeIn(byteString);
	}
	
	public static short[] toShortArray(ByteString byteString) throws ParseException{
		return (short[])serializeIn(byteString);
	}
	
	public static double[] toDoubleArray(ByteString byteString) throws ParseException{
		return (double[])serializeIn(byteString);
	}
	
	public static long[] toLongArray(ByteString byteString) throws ParseException{
		return (long[])serializeIn(byteString);
	}
	
	public static boolean[] toBooleanArray(ByteString byteString) throws ParseException{
		return (boolean[])serializeIn(byteString);
	}
}
