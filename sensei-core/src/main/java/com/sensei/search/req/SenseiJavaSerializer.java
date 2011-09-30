package com.sensei.search.req;

import com.linkedin.norbert.network.Serializer;
import scala.Tuple2;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * This is partially a minor fix for the Java Serializer in Norbert and partially a fix because Sensei uses
 * SenseiRequest for both SenseiRequest and SyseiSysRequests, when maybe they should be split up!
 * @param <RequestMsg>
 * @param <ResponseMsg>
 */
public class SenseiJavaSerializer<RequestMsg, ResponseMsg> implements Serializer<RequestMsg, ResponseMsg> {
	private final Class<RequestMsg> requestClass;
	private final Class<ResponseMsg> responseClass;
	private final String reqName;
	private final String resName;

	private SenseiJavaSerializer(Class<RequestMsg> requestClass, Class<ResponseMsg> responseClass,
	                             String reqName, String resName) {
		this.requestClass = requestClass;
		this.responseClass = responseClass;
		this.reqName = reqName;
		this.resName = resName;
	}

	public static <RequestMsg, ResponseMsg> SenseiJavaSerializer<RequestMsg, ResponseMsg> build
			(String reqName, String resName, Class<RequestMsg> requestClass, Class<ResponseMsg> responseClass) {
		return new SenseiJavaSerializer<RequestMsg, ResponseMsg>(requestClass, responseClass, reqName, resName);
	}

	public static <RequestMsg, ResponseMsg> SenseiJavaSerializer<RequestMsg, ResponseMsg> build
			(Class<RequestMsg> requestClass, Class<ResponseMsg> responseClass) {
		return new SenseiJavaSerializer<RequestMsg, ResponseMsg>(requestClass, responseClass, requestClass.getName(), responseClass.getName());
	}

	public static <RequestMsg, ResponseMsg> SenseiJavaSerializer<RequestMsg, ResponseMsg> build
			(String requestClassName, Class<RequestMsg> requestClass, Class<ResponseMsg> responseClass) {
		return new SenseiJavaSerializer<RequestMsg, ResponseMsg>(requestClass, responseClass, requestClassName, responseClass.getName());
	}

	private <T> byte[] toBytes(T message) {
		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(baos);
			oos.writeObject(message);
			return baos.toByteArray();
		} catch (Exception ex) {
			throw new IllegalStateException("Sensei server not properly serializing objects ", ex);
		}
	}

	private <T> T fromBytes(byte[] bytes) {
		try {
			ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
			ObjectInputStream ois = new ObjectInputStream(bais);
			return (T) ois.readObject();
		} catch (Exception ex) {
			throw new IllegalStateException("Sensei server not properly serializing objects ", ex);
		}
	}

	public String requestName() {
		return reqName;
	}

	public String responseName() {
		return resName;
	}

	public byte[] requestToBytes(RequestMsg message) {
		return toBytes(message);
	}

	public RequestMsg requestFromBytes(byte[] bytes) {
		return fromBytes(bytes);
	}

	public byte[] responseToBytes(ResponseMsg message) {
		return toBytes(message);
	}

	public ResponseMsg responseFromBytes(byte[] bytes) {
		return fromBytes(bytes);
	}
}
