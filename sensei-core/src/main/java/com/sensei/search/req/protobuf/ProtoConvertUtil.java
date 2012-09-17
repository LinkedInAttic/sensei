/**
 * This software is licensed to you under the Apache License, Version 2.0 (the
 * "Apache License").
 *
 * LinkedIn's contributions are made under the Apache License. If you contribute
 * to the Software, the contributions will be deemed to have been made under the
 * Apache License, unless you expressly indicate otherwise. Please do not make any
 * contributions that would be inconsistent with the Apache License.
 *
 * You may obtain a copy of the Apache License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, this software
 * distributed under the Apache License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the Apache
 * License for the specific language governing permissions and limitations for the
 * software governed under the Apache License.
 *
 * © 2012 LinkedIn Corp. All Rights Reserved.  
 */
package com.sensei.search.req.protobuf;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;

import com.google.protobuf.ByteString;
import com.google.protobuf.TextFormat.ParseException;

/**
 * @author nnarkhed
 */
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
			return serializeOut(data);
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
			return serializeOut(data);
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
			return serializeOut(data);
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
			return serializeOut(data);
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
			return serializeOut(data);
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
			return (int[])serializeIn(byteString);
		}
		catch(Exception e){
			logger.error(e.getMessage(),e);
			throw new ParseException(e.getMessage());
		}
	}

    /**
     * This function deserializes the input byte string back into an integer array
     * @param byteString        the byte string serialized by serializeData()
     * @return                  the deserialized integer array
     * @throws ParseException
     */
    public static Set<Integer> toIntegerSet(ByteString byteString) throws ParseException{
        if (byteString==null) return null;
        try{
            int[] data = (int[])serializeIn(byteString);
            Set<Integer> intset = null;
            if (data != null){
              intset = new HashSet<Integer>(data.length);
	          for (int datum : data){
	          	intset.add(datum);
	          }
            }
            return intset;
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
			return (char[])serializeIn(byteString);
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
			return (double[])serializeIn(byteString);
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
			return (long[])serializeIn(byteString);
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
			return (boolean[])serializeIn(byteString);
		}
		catch(Exception e){
			logger.error(e.getMessage(),e);
			throw new ParseException(e.getMessage());
		}
		
	}

	private static byte[] intToByteArray(int data) {
		byte[] bytes = new byte[4];
		bytes[0] = (byte) (data & 0x000F);
		bytes[1] = (byte) ((data & 0x00F0) >> 8);
		bytes[2] = (byte) ((data & 0x0F00) >> 16);
		bytes[3] = (byte) ((data & 0xF000) >> 24);
		return bytes;		
	}
}
