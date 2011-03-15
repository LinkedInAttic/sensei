/**
 * 
 */
package com.sensei.test;

import junit.framework.TestCase;

import com.google.protobuf.ByteString;
import com.google.protobuf.TextFormat.ParseException;
import com.sensei.search.req.protobuf.ProtoConvertUtil;

/**
 * @author nnarkhed
 *
 */
public class TestSerialization extends TestCase{

	public TestSerialization(String testName) {
		super(testName);
	}
	
	/**
	 * This tests the serialization of integer arrays into ByteString and deserialization back into integer array
	 */
	public void testIntegerSerialization() {
		int[] intData = new int[]{0, -2147483645, -18, 10, 1984, 11000, 120000, 2147483647};
		
		try {
			ByteString intDataString = ProtoConvertUtil.serializeData(intData);
			int[] outIntData = ProtoConvertUtil.toIntArray(intDataString);
			assertEquals(intData.length, outIntData.length);
			for(int i = 0;i < intData.length;i ++) {
				assertEquals(intData[i], outIntData[i]);				
			}
		} catch (ParseException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}		
	}
	
	/**
	 * This tests the serialization of double arrays into ByteString and deserialization back into double array
	 */
	public void testDoubleSerialization() {
		double[] doubleData = new double[]{0.0d, Double.longBitsToDouble(0x7fefffffffffffffL), 
				Double.longBitsToDouble(-0x7fefffffffffffffL)};
		
		try {
			ByteString doubleDataString = ProtoConvertUtil.serializeData(doubleData);
			double[] outDoubleData = ProtoConvertUtil.toDoubleArray(doubleDataString);
			assertEquals(doubleData.length, outDoubleData.length);
			for(int i = 0;i < doubleData.length;i ++) {
				assertEquals(doubleData[i], outDoubleData[i]);				
			}
		}catch(ParseException pe) {
			pe.printStackTrace();
			fail(pe.getMessage());
		}		
	}

	/**
	 * This tests the serialization of long arrays into ByteString and deserialization back into long array
	 */
	public void testLongSerialization() {
		long[] longData = new long[]{0, 1000000000000000L, 2000000000000000L, 311111112222222L, -5000000000000000L};
		
		try {
			ByteString longDataString = ProtoConvertUtil.serializeData(longData);
			long[] outLongData = ProtoConvertUtil.toLongArray(longDataString);
			assertEquals(longData.length, outLongData.length);
			for(int i = 0;i < longData.length;i ++) {
				assertEquals(longData[i], outLongData[i]);				
			}
		}catch (ParseException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}		
	}

	/**
	 * This tests the serialization of boolean arrays into ByteString and deserialization back into boolean array
	 */
	public void testBooleanSerialization() {
		boolean[] boolData = new boolean[]{true, false, false, true};
		try {
			ByteString boolDataString = ProtoConvertUtil.serializeData(boolData);
			boolean[] outBoolData = ProtoConvertUtil.toBooleanArray(boolDataString);
			assertEquals(boolData.length, outBoolData.length);
			for(int i = 0;i < boolData.length;i ++) {
				assertEquals(boolData[i], outBoolData[i]);				
			}
		} catch (ParseException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}		
	}

	/**
	 * This tests the serialization of character arrays into ByteString and deserialization back into 
	 * character array
	 */
	public void testCharSerialization() {
		char[] charData = new char[]{'s', 'e', 'n', 's', 'e', 'i'};
		try {
			ByteString charDataString = ProtoConvertUtil.serializeData(charData);
			char[] outcharData = ProtoConvertUtil.toCharArray(charDataString);
			assertEquals(charData.length, outcharData.length);
			for(int i = 0;i < charData.length;i ++) {
				assertEquals(charData[i], outcharData[i]);				
			}
		} catch (ParseException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}		
	}
}
