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
package com.senseidb.svc.impl;

import java.io.IOException;
import java.util.List;

import com.sensei.search.req.protobuf.SenseiSysReqProtoSerializer;
import org.apache.log4j.Logger;

import com.browseengine.bobo.api.BoboBrowser;
import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.api.MultiBoboBrowser;
import com.linkedin.norbert.network.JavaSerializer;
import com.linkedin.norbert.network.Serializer;
import com.senseidb.search.node.SenseiCore;
import com.senseidb.search.node.SenseiQueryBuilderFactory;
import com.senseidb.search.req.SenseiRequest;
import com.senseidb.search.req.SenseiSystemInfo;

public class SysSenseiCoreServiceImpl extends AbstractSenseiCoreService<SenseiRequest, SenseiSystemInfo>{
	public static final Serializer<SenseiRequest, SenseiSystemInfo> JAVA_SERIALIZER =
			JavaSerializer.apply("SenseiSysRequest", SenseiRequest.class, SenseiSystemInfo.class);

	public static final Serializer<SenseiRequest, SenseiSystemInfo> PROTO_SERIALIZER =
			new SenseiSysReqProtoSerializer();

  private static final Logger logger = Logger.getLogger(SysSenseiCoreServiceImpl.class);
  
  public SysSenseiCoreServiceImpl(SenseiCore core) {
    super(core);
  }
  
  @Override
  public SenseiSystemInfo handlePartitionedRequest(SenseiRequest request,
      List<BoboIndexReader> readerList,SenseiQueryBuilderFactory queryBuilderFactory) throws Exception {
    SenseiSystemInfo res = new SenseiSystemInfo();

    MultiBoboBrowser browser = null;
    try
    {
      browser = new MultiBoboBrowser(BoboBrowser.createBrowsables(readerList));
      res.setNumDocs(browser.numDocs());

      return res;
    }
    catch (Exception e)
    {
      logger.error(e.getMessage(), e);
      throw e;
    }
    finally
    {
      if (browser != null)
      {
        try
        {
          browser.close();
        } catch (IOException ioe)
        {
          logger.error(ioe.getMessage(), ioe);
        }
      }
    }
  }

  @Override
  public SenseiSystemInfo mergePartitionedResults(SenseiRequest r,
      List<SenseiSystemInfo> resultList) {
    SenseiSystemInfo result = _core.getSystemInfo();
    result.setNumDocs(0);
    for (SenseiSystemInfo res : resultList)
    {
      result.setNumDocs(result.getNumDocs() + res.getNumDocs());
    }

    return result;
  }

	@Override
	public SenseiSystemInfo getEmptyResultInstance(Throwable error) {
		return new SenseiSystemInfo();
	}

	@Override
	public Serializer<SenseiRequest, SenseiSystemInfo> getSerializer() {
		return PROTO_SERIALIZER;
	}
}

