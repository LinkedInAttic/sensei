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

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;
import com.linkedin.norbert.network.Serializer;
import com.senseidb.search.req.SenseiRequest;
import com.senseidb.search.req.SenseiSystemInfo;

public class SenseiSysReqProtoSerializer implements Serializer<SenseiRequest, SenseiSystemInfo> {
	public String requestName() {
		return SenseiSysRequestBPO.getDescriptor().getName();
	}

	public String responseName() {
		return SenseiSysResultBPO.getDescriptor().getName();
	}

	public byte[] requestToBytes(SenseiRequest request) {
		return SenseiRequestBPO.Request.newBuilder().setVal(serialize(request)).build().toByteArray();
	}

	public byte[] responseToBytes(SenseiSystemInfo response) {
		return SenseiSysResultBPO.SysResult.newBuilder().setVal(serialize(response)).build().toByteArray();
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

	public SenseiSystemInfo responseFromBytes(byte[] result) {
		try {
			return (SenseiSystemInfo) ProtoConvertUtil.serializeIn(SenseiSysResultBPO.SysResult.newBuilder().mergeFrom(result).build().getVal());
		} catch (TextFormat.ParseException e) {
			throw new RuntimeException(e);
		} catch (InvalidProtocolBufferException e) {
			throw new RuntimeException(e);
		}
	}
}
