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
package com.senseidb.search.req;


import com.linkedin.norbert.network.Serializer;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.iq80.snappy.SnappyInputStream;
import org.iq80.snappy.SnappyOutputStream;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;


/**
 * Takes any Serializer and compresses/decompresses the data.
 */
public class SenseiSnappySerializer<RequestType, ResponseType> implements Serializer<RequestType, ResponseType> {
  private final static Logger logger = Logger.getLogger(SenseiSnappySerializer.class);

  private final Serializer<RequestType, ResponseType> _inner;
  private final String requestName;
  private final String responseName;

  public SenseiSnappySerializer(Serializer<RequestType, ResponseType> inner) {
    _inner = inner;
    requestName = "SnappyRequest(" + (inner.requestName() == null ? "" : inner.requestName()) + ")";
    responseName = "SnappyResponse(" + (inner.responseName() == null ? "" : inner.responseName())  + ")";
  }

  public static <RequestType, ResponseType> SenseiSnappySerializer<RequestType, ResponseType> wrap(Serializer<RequestType, ResponseType> inner) {
    return new SenseiSnappySerializer<RequestType, ResponseType>(inner);
  }

  @Override
  public String requestName() {
    return requestName;
  }

  @Override
  public String responseName() {
    return responseName;
  }

  @Override
  public RequestType requestFromBytes(byte[] compressedBytes) {
    ByteArrayInputStream bais = new ByteArrayInputStream(compressedBytes);

    byte[] uncompressedBytes = null;
    try {
      SnappyInputStream snappyInputStream = new SnappyInputStream(bais);
      uncompressedBytes = IOUtils.toByteArray(snappyInputStream);
    } catch (IOException e) {
      // This should not happen
      logger.warn("Could not decompress sensei request", e);
    }

    RequestType request = _inner.requestFromBytes(uncompressedBytes);

//    // For debugging serialization
//    byte[] bytes2 = _inner.requestToBytes(request);
//    RequestType request2 = _inner.requestFromBytes(bytes2);
//    if(!request.equals(request2)) {
//      throw new IllegalArgumentException();
//    }

    return request;
  }

  @Override
  public ResponseType responseFromBytes(byte[] compressedBytes) {
    ByteArrayInputStream bais = new ByteArrayInputStream(compressedBytes);

    byte[] uncompressedBytes = null;
    try {
      SnappyInputStream snappyInputStream = new SnappyInputStream(bais);
      uncompressedBytes = IOUtils.toByteArray(snappyInputStream);
    } catch (IOException e) {
      // This should not happen
      logger.warn("Could not decompress sensei request", e);
    }

    ResponseType response = _inner.responseFromBytes(uncompressedBytes);

//    // For debugging serialization
//    byte[] bytes = _inner.responseToBytes(response);
//    ResponseType response2 = _inner.responseFromBytes(bytes);
//    if(!response.equals(response2)) {
//      throw new IllegalArgumentException();
//    }

    return response;
  }

  @Override
  public byte[] requestToBytes(RequestType request) {
    byte[] uncompressedBytes = _inner.requestToBytes(request);

//    // For debugging serialization
//    RequestType request2 = _inner.requestFromBytes(uncompressedBytes);
//    if(!request.equals(request2)) {
//      throw new IllegalArgumentException();
//    }

    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    try {
      SnappyOutputStream snappyOutputStream = new SnappyOutputStream(baos);
      snappyOutputStream.write(uncompressedBytes);
      snappyOutputStream.close();
      baos.close();
    } catch (IOException e) {
      // This should not happen
      logger.error("Could not compress sensei request ", e);
    }

    return baos.toByteArray();
  }

  @Override
  public byte[] responseToBytes(ResponseType response) {
    byte[] uncompressedBytes = _inner.responseToBytes(response);

//    // For debugging serialization
//    ResponseType response2 = _inner.responseFromBytes(uncompressedBytes);
//    if(!response.equals(response2)) {
//      throw new IllegalArgumentException();
//    }

    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    try {
      SnappyOutputStream snappyOutputStream = new SnappyOutputStream(baos);
      snappyOutputStream.write(uncompressedBytes);
      snappyOutputStream.close();
      baos.close();
    } catch (IOException e) {
      // This should not happen
      logger.error("Could not compress sensei request ", e);
    }
    return baos.toByteArray();
  }
}
