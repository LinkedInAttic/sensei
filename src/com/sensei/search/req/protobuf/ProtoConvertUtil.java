package com.sensei.search.req.protobuf;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
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

	/**
	 * serialize out an int array
	 * @param data   input integer array
	 * @return       serialized integer array in the format - <len><int><int>....
	 * @throws ParseException
	 */
	public static ByteString serializeData(int[] data) throws ParseException{
		if (data == null) return null;
		try{
			int bytesIndex = 0;
			// each integer is encoded as 4 bytes + 4 more bytes for the length of the array
			byte[] bytes = new byte[data.length*4 + 4];
			// write out the length before the data
			bytes[bytesIndex++] = (byte) ((data.length >>> 24) & 0xFF);
			bytes[bytesIndex++] = (byte) ((data.length >>> 16) & 0xFF);
			bytes[bytesIndex++] = (byte) ((data.length >>> 8) & 0xFF);
			bytes[bytesIndex++] = (byte) ((data.length >>> 0) & 0xFF);

			// write out integer data one by one
			for(int datum : data) {
				bytes[bytesIndex++] = (byte) ((datum >>> 24) & 0xFF);
				bytes[bytesIndex++] = (byte) ((datum >>> 16) & 0xFF);
				bytes[bytesIndex++] = (byte) ((datum >>> 8) & 0xFF);
				bytes[bytesIndex++] = (byte) ((datum >>> 0) & 0xFF);
			}
			return ByteString.copyFrom(bytes);
		}
		catch(Exception e){
			logger.error(e.getMessage(),e);
			throw new ParseException(e.getMessage());
		}
	}

	/**
	 * serialize out an boolean array
	 * @param data   input boolean array
	 * @return       serialized boolean array in the format - <len><boolean><boolean>...
	 * @throws ParseException
	 */
	public static ByteString serializeData(boolean[] data) throws ParseException{
		if (data == null) return null;
		try{
			int bytesIndex = 0;
			// each integer is encoded as one byte + 4 more bytes for the length of the array
			byte[] bytes = new byte[data.length + 4];
			// write out the length before the data
			bytes[bytesIndex++] = (byte) ((data.length >>> 24) & 0xFF);
			bytes[bytesIndex++] = (byte) ((data.length >>> 16) & 0xFF);
			bytes[bytesIndex++] = (byte) ((data.length >>> 8) & 0xFF);
			bytes[bytesIndex++] = (byte) ((data.length >>> 0) & 0xFF);

			// write out boolean data one by one
			for(boolean datum : data) {
				if(datum)
					bytes[bytesIndex++] = 1;
				else
					bytes[bytesIndex++] = 0;
			}
			return ByteString.copyFrom(bytes);
		}
		catch(Exception e){
			logger.error(e.getMessage(),e);
			throw new ParseException(e.getMessage());
		}
	}

	/**
	 * serialize out an char array
	 * @param data   input character array
	 * @return       serialized char array in the format - <len><char><char>...
	 * @throws 		 ParseException
	 */
	public static ByteString serializeData(char[] data) throws ParseException{
		if (data == null) return null;
		try{
			int bytesIndex = 0;
			// each character is encoded as 2 bytes + 4 more bytes for the length of the array
			byte[] bytes = new byte[data.length*2 + 4];
			
			// write out the length before the data
			bytes[bytesIndex++] = (byte) ((data.length >>> 24) & 0xFF);
			bytes[bytesIndex++] = (byte) ((data.length >>> 16) & 0xFF);
			bytes[bytesIndex++] = (byte) ((data.length >>> 8) & 0xFF);
			bytes[bytesIndex++] = (byte) ((data.length >>> 0) & 0xFF);

			// write out character data one by one
			for(int datum : data) {
				bytes[bytesIndex++] = (byte) ((datum >>> 8) & 0xFF);
				bytes[bytesIndex++] = (byte) ((datum >>> 0) & 0xFF);
			}
			return ByteString.copyFrom(bytes);
		}
		catch(Exception e){
			logger.error(e.getMessage(),e);
			throw new ParseException(e.getMessage());
		}
	}

	/**
	 * serialize out an long array
	 * @param data   input long array
	 * @return       serialized long array in the format - <len><long><long>...
	 * @throws ParseException
	 */
	public static ByteString serializeData(long[] data) throws ParseException{
		if (data == null) return null;
		try{
			int bytesIndex = 0;
			// each integer is encoded as 4 bytes + 4 more bytes for the length of the array
			byte[] bytes = new byte[data.length*8 + 4];

			// write out the length before the data
			bytes[bytesIndex++] = (byte) ((data.length >>> 24) & 0xFF);
			bytes[bytesIndex++] = (byte) ((data.length >>> 16) & 0xFF);
			bytes[bytesIndex++] = (byte) ((data.length >>> 8) & 0xFF);
			bytes[bytesIndex++] = (byte) ((data.length >>> 0) & 0xFF);

			// write out integer data one by one
			for(long datum : data) {
				bytes[bytesIndex++] = (byte) ((datum >>> 56) & 0xFF);
				bytes[bytesIndex++] = (byte) ((datum >>> 48) & 0xFF);
				bytes[bytesIndex++] = (byte) ((datum >>> 40) & 0xFF);
				bytes[bytesIndex++] = (byte) ((datum >>> 32) & 0xFF);
				bytes[bytesIndex++] = (byte) ((datum >>> 24) & 0xFF);
				bytes[bytesIndex++] = (byte) ((datum >>> 16) & 0xFF);
				bytes[bytesIndex++] = (byte) ((datum >>> 8) & 0xFF);
				bytes[bytesIndex++] = (byte) ((datum >>> 0) & 0xFF);
			}
			return ByteString.copyFrom(bytes);
		}
		catch(Exception e){
			logger.error(e.getMessage(),e);
			throw new ParseException(e.getMessage());
		}
	}

	/**
	 * serialize out a double array
	 * @param data   input double array
	 * @return       serialized double array in the format - <len><double><double>...
	 * @throws ParseException
	 */
	public static ByteString serializeData(double[] data) throws ParseException{
		if (data == null) return null;
		try{
			int bytesIndex = 0;
			// each integer is encoded as 4 bytes + 4 more bytes for the length of the array
			byte[] bytes = new byte[data.length*8 + 4];

			// write out the length before the data
			bytes[bytesIndex++] = (byte) ((data.length >>> 24) & 0xFF);
			bytes[bytesIndex++] = (byte) ((data.length >>> 16) & 0xFF);
			bytes[bytesIndex++] = (byte) ((data.length >>> 8) & 0xFF);
			bytes[bytesIndex++] = (byte) ((data.length >>> 0) & 0xFF);

			// write out the length before the data
			// write out long data one by one
			for(double doubleData : data) {
				long datum = Double.doubleToLongBits(doubleData);
				bytes[bytesIndex++] = (byte) ((datum >>> 56) & 0xFF);
				bytes[bytesIndex++] = (byte) ((datum >>> 48) & 0xFF);
				bytes[bytesIndex++] = (byte) ((datum >>> 40) & 0xFF);
				bytes[bytesIndex++] = (byte) ((datum >>> 32) & 0xFF);
				bytes[bytesIndex++] = (byte) ((datum >>> 24) & 0xFF);
				bytes[bytesIndex++] = (byte) ((datum >>> 16) & 0xFF);
				bytes[bytesIndex++] = (byte) ((datum >>> 8) & 0xFF);
				bytes[bytesIndex++] = (byte) ((datum >>> 0) & 0xFF);
			}
			return ByteString.copyFrom(bytes);
		}
		catch(Exception e){
			logger.error(e.getMessage(),e);
			throw new ParseException(e.getMessage());
		}
	}

	/**
	 * This function deserializes the input byte string back into an integer array
	 * @param byteString	 	the byte string serialized by serializeData()
	 * @return					the deserialized integer array
	 * @throws ParseException
	 */
	public static int[] toIntArray(ByteString byteString) throws ParseException{
		if (byteString==null) return null;
		try{
			int bytesIndex = 0;
			byte[] bytes = byteString.toByteArray();
			if ( (bytes.length==0) || (bytes.length < 4) ) return null;

			// deserialize the count of the integer array first
			int bytesCount = bytes.length;
			int dataLength = 0;
			int d1 = (0x000000FF & (int)bytes[0]);
			int d2 = (0x000000FF & (int)bytes[1]);
			int d3 = (0x000000FF & (int)bytes[2]);
			int d4 = (0x000000FF & (int)bytes[3]);
			bytesIndex += 4;
			if( (d1 | d2 | d3 | d4) < 0 )
				throw new ParseException("Stream has invalid data");
			dataLength = ( (d1 << 24) + (d2 << 16) + (d3 << 8) + (d4 << 0));
			System.out.println("Deserializing length : " + dataLength);
			
			// allocate the integer array
			int[] data = new int[dataLength];
			int dataIndex = 0;
			while( (bytesIndex+4) <= bytesCount) {
				d1 = (0x000000FF & (int)bytes[bytesIndex++]);
				d2 = (0x000000FF & (int)bytes[bytesIndex++]);
				d3 = (0x000000FF & (int)bytes[bytesIndex++]);
				d4 = (0x000000FF & (int)bytes[bytesIndex++]);
//				unsignedInt = ((long) (d1 << 24 | d2 << 16 | d3 << 8 | d4)) & 0xFFFFFFFFL;
				data[dataIndex++] = ( (d1 << 24) + (d2 << 16) + (d3 << 8) + (d4 << 0) );
				System.out.println("Deserializing : " + data[dataIndex-1]);
			}
			return data;
		}
		catch(Exception e){
			logger.error(e.getMessage(),e);
			throw new ParseException(e.getMessage());
		}
	}

	public static char[] toCharArray(ByteString byteString) throws ParseException{
		//		return (short[])serializeIn(byteString);
		if (byteString==null) return null;
		try{
			int bytesIndex = 0;
			byte[] bytes = byteString.toByteArray();
			if ( (bytes.length==0) || (bytes.length < 4) ) return null;

			// deserialize the count of the char array first
			int bytesCount = bytes.length;
			int dataLength = 0;
			int d1 = (0x000000FF & (int)bytes[0]);
			int d2 = (0x000000FF & (int)bytes[1]);
			int d3 = (0x000000FF & (int)bytes[2]);
			int d4 = (0x000000FF & (int)bytes[3]);
			bytesIndex += 4;
			if( (d1 | d2 | d3 | d4) < 0 )
				throw new ParseException("Stream has invalid data");
			dataLength = ( (d1 << 24) + (d2 << 16) + (d3 << 8) + (d4 << 0));
			
			// allocate the char array
			char[] data = new char[dataLength];
			int dataIndex = 0;
			while( (bytesIndex+2) <= bytesCount) {
				d1 = bytes[bytesIndex++];
				d2 = bytes[bytesIndex++];
				if( (d1 | d2) < 0 )
					throw new ParseException("Stream has invalid data");
				data[dataIndex++] = (char) ( (d1 << 8) + (d2 << 0) );
			}
			return data;
		}
		catch(Exception e){
			logger.error(e.getMessage(),e);
			throw new ParseException(e.getMessage());
		}
	}

	/**
	 * This function deserializes the input byte string back into a double array
	 * @param byteString	 	the byte string serialized by serializeData()
	 * @return					the deserialized double array
	 * @throws ParseException
	 */
	public static double[] toDoubleArray(ByteString byteString) throws ParseException{
		//		return (double[])serializeIn(byteString);
		if (byteString==null) return null;
		try{
			int bytesIndex = 0;
			byte[] bytes = byteString.toByteArray();
			if ( (bytes.length==0) || (bytes.length < 4) ) return null;

			// deserialize the count of the double array first
			int bytesCount = bytes.length;
			int dataLength = 0;
			int d1 = (0x000000FF & (int)bytes[0]);
			int d2 = (0x000000FF & (int)bytes[1]);
			int d3 = (0x000000FF & (int)bytes[2]);
			int d4 = (0x000000FF & (int)bytes[3]);
			bytesIndex += 4;
			if( (d1 | d2 | d3 | d4) < 0 )
				throw new ParseException("Stream has invalid data");
			dataLength = ( (d1 << 24) + (d2 << 16) + (d3 << 8) + (d4 << 0));
			
			// allocate the double array
			double[] data = new double[dataLength];
			int d5, d6, d7, d8;
			int dataIndex = 0;
			long longData = 0;
			while( (bytesIndex+8) <= bytesCount) {
				d1 = bytes[bytesIndex++];
				d2 = bytes[bytesIndex++];
				d3 = bytes[bytesIndex++];
				d4 = bytes[bytesIndex++];
				d5 = bytes[bytesIndex++];
				d6 = bytes[bytesIndex++];
				d7 = bytes[bytesIndex++];
				d8 = bytes[bytesIndex++];
				longData = ( ((long)d1 << 56) + 
							 ((long)(d2 & 0xFF) << 48) + 
							 ((long)(d3 & 0xFF) << 40) + 
							 ((long)(d4 & 0xFF) << 32) + 
							 ((long)(d5 & 0xFF) << 24) + 
							 ((d6 & 0xFF) << 16) + 
							 ((d7 & 0xFF) << 8) + 
							 ((d8 & 0xFF) << 0) );
				data[dataIndex++] = Double.longBitsToDouble(longData);
			}
			return data;
		}
		catch(Exception e){
			logger.error(e.getMessage(),e);
			throw new ParseException(e.getMessage());
		}
	}

	/**
	 * This function deserializes the input byte string back into an long array
	 * @param byteString	 	the byte string serialized by serializeData()
	 * @return					the deserialized long array
	 * @throws ParseException
	 */
	public static long[] toLongArray(ByteString byteString) throws ParseException{
		//		return (long[])serializeIn(byteString);
		if (byteString==null) return null;
		try{
			int bytesIndex = 0;
			byte[] bytes = byteString.toByteArray();
			if ( (bytes.length==0) || (bytes.length < 4) ) return null;

			// deserialize the count of the long array first
			int bytesCount = bytes.length;
			int dataLength = 0;
			int d1 = (0x000000FF & (int)bytes[0]);
			int d2 = (0x000000FF & (int)bytes[1]);
			int d3 = (0x000000FF & (int)bytes[2]);
			int d4 = (0x000000FF & (int)bytes[3]);
			bytesIndex += 4;
			if( (d1 | d2 | d3 | d4) < 0 )
				throw new ParseException("Stream has invalid data");
			dataLength = ( (d1 << 24) + (d2 << 16) + (d3 << 8) + (d4 << 0));
			
			// allocate the long array
			long[] data = new long[dataLength];
			int d5, d6, d7, d8;
			int dataIndex = 0;
			while( (bytesIndex+8) <= bytesCount) {
				d1 = bytes[bytesIndex++];
				d2 = bytes[bytesIndex++];
				d3 = bytes[bytesIndex++];
				d4 = bytes[bytesIndex++];
				d5 = bytes[bytesIndex++];
				d6 = bytes[bytesIndex++];
				d7 = bytes[bytesIndex++];
				d8 = bytes[bytesIndex++];
				data[dataIndex++] = ( ((long)d1 << 56) + 
									  ((long)(d2 & 0xFF) << 48) + 
									  ((long)(d3 & 0xFF) << 40) + 
									  ((long)(d4 & 0xFF) << 32) + 
									  ((long)(d5 & 0xFF) << 24) + 
									  ((d6 & 0xFF) << 16) + 
									  ((d7 & 0xFF) << 8) + 
									  ((d8 & 0xFF) << 0) );				
			}
			return data;
		}
		catch(Exception e){
			logger.error(e.getMessage(),e);
			throw new ParseException(e.getMessage());
		}
	}

	/**
	 * This function deserializes the input byte string back into a boolean array
	 * @param byteString	 	the byte string serialized by serializeData()
	 * @return					the deserialized boolean array
	 * @throws ParseException
	 */
	public static boolean[] toBooleanArray(ByteString byteString) throws ParseException{
		if (byteString==null) return null;
		try{
			int bytesIndex = 0;
			byte[] bytes = byteString.toByteArray();
			if ( (bytes.length==0) || (bytes.length < 4) ) return null;

			// deserialize the count of the boolean array first
			int bytesCount = bytes.length;
			int dataLength = 0;
			int d1 = (0x000000FF & (int)bytes[0]);
			int d2 = (0x000000FF & (int)bytes[1]);
			int d3 = (0x000000FF & (int)bytes[2]);
			int d4 = (0x000000FF & (int)bytes[3]);
			bytesIndex += 4;
			if( (d1 | d2 | d3 | d4) < 0 )
				throw new ParseException("Stream has invalid data");
			dataLength = ( (d1 << 24) + (d2 << 16) + (d3 << 8) + (d4 << 0));
			
			// allocate the boolean array
			boolean[] data = new boolean[dataLength];
			int dataIndex = 0;
			while(bytesIndex < bytesCount) {
				byte datum = bytes[bytesIndex++];
				if( datum < 0 )
					throw new ParseException("Stream has invalid data");
				data[dataIndex++] = (datum != 0);
			}
			return data;
		}
		catch(Exception e){
			logger.error(e.getMessage(),e);
			throw new ParseException(e.getMessage());
		}
		
	}

	public static byte[] intToByteArray(int data) {
		byte[] bytes = new byte[4];
		bytes[0] = (byte) (data & 0x000F);
		bytes[1] = (byte) ((data & 0x00F0) >> 8);
		bytes[2] = (byte) ((data & 0x0F00) >> 16);
		bytes[3] = (byte) ((data & 0xF000) >> 24);
		return bytes;		
	}
}
