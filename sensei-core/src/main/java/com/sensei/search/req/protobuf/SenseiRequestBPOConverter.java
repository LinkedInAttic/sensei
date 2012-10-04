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
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import com.senseidb.search.req.SenseiRequest;
import com.senseidb.search.req.SenseiResult;
import org.apache.log4j.Logger;
import com.google.protobuf.ByteString;

public class SenseiRequestBPOConverter {

	private static Logger logger = Logger.getLogger(SenseiRequestBPOConverter.class);
	
  public static SenseiRequest convert(SenseiRequestBPO.Request req)
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
  public static SenseiRequestBPO.Request convert(SenseiRequest req)
  {
    SenseiRequestBPO.Request.Builder builder = SenseiRequestBPO.Request.newBuilder();
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
    return SenseiRequestBPO.Request.getDefaultInstance();
  }
  public static SenseiResult convert(SenseiResultBPO.Result req)
  {
    try
    {
      ByteString value = req.getVal();
      byte[] raw = value.toByteArray();
      ByteArrayInputStream bais = new ByteArrayInputStream(raw);
      ObjectInputStream ois = new ObjectInputStream(bais);
      SenseiResult ret = (SenseiResult) ois.readObject();
      return ret;
    } catch (Exception e)
    {
      logger.error("serialize result", e);
    }
    return null;
  }
  public static SenseiResultBPO.Result convert(SenseiResult req)
  {
    SenseiResultBPO.Result.Builder builder = SenseiResultBPO.Result.newBuilder();
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
      logger.error("deserialize result", e);
    }
    return SenseiResultBPO.Result.getDefaultInstance();
  }
}
