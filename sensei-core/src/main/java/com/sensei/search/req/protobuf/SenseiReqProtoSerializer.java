package com.sensei.search.req.protobuf;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;
import com.linkedin.norbert.network.Serializer;
import com.senseidb.search.req.SenseiRequest;
import com.senseidb.search.req.SenseiResult;

public class SenseiReqProtoSerializer implements Serializer<SenseiRequest, SenseiResult> {
	public String requestName() {
		return SenseiRequestBPO.Request.getDescriptor().getName();
	}

	public String responseName() {
		return SenseiResultBPO.Result.getDescriptor().getName();
	}

	public byte[] requestToBytes(SenseiRequest request) {
		return SenseiRequestBPO.Request.newBuilder().setVal(serialize(request)).build().toByteArray();
	}

	public byte[] responseToBytes(SenseiResult response) {
		return SenseiResultBPO.Result.newBuilder().setVal(serialize(response)).build().toByteArray();
	}

	private <T> ByteString serialize(T obj) {
		try {
			return ProtoConvertUtil.serializeOut(obj);
		} catch (TextFormat.ParseException e) {
			throw new RuntimeException(e);
		}
	}

	public SenseiRequest requestFromBytes(byte[] request) {
		try {
			return (SenseiRequest) ProtoConvertUtil.serializeIn(SenseiRequestBPO.Request.newBuilder().mergeFrom(request).build().getVal());
		} catch (TextFormat.ParseException e) {
			throw new RuntimeException(e);
		} catch (InvalidProtocolBufferException e) {
			throw new RuntimeException(e);
		}
	}

	public SenseiResult responseFromBytes(byte[] result) {
		try {
			return (SenseiResult) ProtoConvertUtil.serializeIn(SenseiResultBPO.Result.newBuilder().mergeFrom(result).build().getVal());
		} catch (TextFormat.ParseException e) {
			throw new RuntimeException(e);
		} catch (InvalidProtocolBufferException e) {
			throw new RuntimeException(e);
		}
	}
}
